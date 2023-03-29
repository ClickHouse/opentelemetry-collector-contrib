// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhouseexporter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestLogsExporter_New(t *testing.T) {
	type validate func(*testing.T, *logsExporter, error)

	_ = func(t *testing.T, exporter *logsExporter, err error) {
		require.Nil(t, err)
		require.NotNil(t, exporter)
	}

	_ = func(want error) validate {
		return func(t *testing.T, exporter *logsExporter, err error) {
			require.Nil(t, exporter)
			require.NotNil(t, err)
			if !errors.Is(err, want) {
				t.Fatalf("Expected error '%v', but got '%v'", want, err)
			}
		}
	}

	failWithMsg := func(msg string) validate {
		return func(t *testing.T, exporter *logsExporter, err error) {
			require.NotNil(t, err)
			require.Contains(t, err.Error(), msg)
		}
	}

	tests := map[string]struct {
		config *Config
		want   validate
	}{
		"no dsn": {
			config: withDefaultConfig(),
			want:   failWithMsg("parse dsn address failed"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			var err error
			exporter, err := newLogsExporter(zap.NewNop(), test.config)
			err = multierr.Append(err, err)

			if exporter != nil {
				err = multierr.Append(err, exporter.start(context.TODO(), nil))
				defer func() {
					require.NoError(t, exporter.shutdown(context.TODO()))
				}()
			}

			test.want(t, exporter, err)
		})
	}
}

func TestExporter_pushLogsData(t *testing.T) {
	t.Run("push success", func(t *testing.T) {

		inlineInsertSQL := renderInlineInsertLogsSQL(withDefaultConfig())

		var serviceName string
		insertValuesArray := make([]string, 10)
		resAttr := make(map[string]string)
		resourceLogs := simpleLogs(10).ResourceLogs()
		for i := 0; i < resourceLogs.Len(); i++ {
			logs := resourceLogs.At(i)
			res := logs.Resource()

			attrs := res.Attributes()
			attributesToMap(attrs, resAttr)

			if v, ok := attrs.Get(conventions.AttributeServiceName); ok {
				serviceName = v.Str()
			}

			for j := 0; j < logs.ScopeLogs().Len(); j++ {
				rs := logs.ScopeLogs().At(j).LogRecords()
				for k := 0; k < rs.Len(); k++ {
					r := rs.At(k)

					logAttr := make(map[string]string, attrs.Len())
					attributesToMap(r.Attributes(), logAttr)

					values, err := prepareValues(r, serviceName, resAttr, logAttr)
					require.NoError(t, err)
					insertValuesArray[k] = values
				}
			}
		}
		formattedInsertQuery := formatInsert(insertValuesArray, inlineInsertSQL)
		require.NotEmpty(t, formattedInsertQuery)
		/*
			INSERT INTO otel_logs (
			                        Timestamp,
			                        TraceId,
			                        SpanId,
			                        TraceFlags,
			                        SeverityText,
			                        SeverityNumber,
			                        ServiceName,
			                        Body,
			                        ResourceAttributes,
			                        LogAttributes
			                        ) VALUES(Tue Mar 28 07:40:28 UTC 2023, , , 0, , 0, , , {}, {"service.name":"v"}),
			                                 (Tue Mar 28 07:40:28 UTC 2023, , , 0, , 0, , , {}, {"service.name":"v"}),
			                                 (Tue Mar 28 07:40:28 UTC 2023, , , 0, , 0, , , {}, {"service.name":"v"})


		*/
	})
}

func TestExporter_prepareValues(t *testing.T) {
	t.Run("push success", func(t *testing.T) {
		var items int
		initClickhouseTestServer(t, func(query string, values []driver.Value) error {
			t.Logf(query)
			t.Logf("%d, values:%+v", items, values)
			if strings.HasPrefix(query, "INSERT") {
				items++
			}
			return nil
		})

		exporter := newTestLogsExporter(t, defaultEndpoint)
		mustPushLogsData(t, exporter, simpleLogs(1))
		mustPushLogsData(t, exporter, simpleLogs(2))
	})
}

func newTestLogsExporter(t *testing.T, dsn string, fns ...func(*Config)) *logsExporter {
	cfg := withTestExporterConfig(fns...)(dsn)
	exporter, err := newLogsExporter(zaptest.NewLogger(t), cfg)
	require.NoError(t, err)

	// need to use the dummy driver driver for testing
	exporter.client, err = newClickHouseClient(cfg)
	require.NoError(t, err)
	require.NoError(t, exporter.start(context.TODO(), nil))

	t.Cleanup(func() { _ = exporter.shutdown(context.TODO()) })
	return exporter
}

func withTestExporterConfig(fns ...func(*Config)) func(string) *Config {
	return func(endpoint string) *Config {
		var configMods []func(*Config)
		configMods = append(configMods, func(cfg *Config) {
			cfg.Endpoint = endpoint
		})
		configMods = append(configMods, fns...)
		return withDefaultConfig(configMods...)
	}
}

func simpleLogs(count int) plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < count; i++ {
		r := sl.LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		r.Attributes().PutStr(conventions.AttributeServiceName, "v")
	}
	return logs
}

func mustPushLogsData(t *testing.T, exporter *logsExporter, ld plog.Logs) {
	err := exporter.pushNativeLogsData(context.TODO(), ld)
	require.NoError(t, err)
}

func initClickhouseTestServer(t *testing.T, recorder recorder) {
	driverName = t.Name()
	sql.Register(t.Name(), &testClickhouseDriver{
		recorder: recorder,
	})
}

type recorder func(query string, values []driver.Value) error

type testClickhouseDriver struct {
	recorder recorder
}

func (t *testClickhouseDriver) Open(name string) (driver.Conn, error) {
	return &testClickhouseDriverConn{
		recorder: t.recorder,
	}, nil
}

type testClickhouseDriverConn struct {
	recorder recorder
}

func (t *testClickhouseDriverConn) Prepare(query string) (driver.Stmt, error) {
	return &testClickhouseDriverStmt{
		query:    query,
		recorder: t.recorder,
	}, nil
}

func (*testClickhouseDriverConn) Close() error {
	return nil
}

func (*testClickhouseDriverConn) Begin() (driver.Tx, error) {
	return &testClickhouseDriverTx{}, nil
}

func (*testClickhouseDriverConn) CheckNamedValue(v *driver.NamedValue) error {
	return nil
}

type testClickhouseDriverStmt struct {
	query    string
	recorder recorder
}

func (*testClickhouseDriverStmt) Close() error {
	return nil
}

func (t *testClickhouseDriverStmt) NumInput() int {
	if !strings.HasPrefix(t.query, `INSERT`) {
		return 0
	}

	n := strings.Count(t.query, "?")
	if n > 0 {
		return n
	}

	// no ? in batched queries but column are separated with ","
	// except for the last one
	return strings.Count(t.query, ",") + 1
}

func (t *testClickhouseDriverStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, t.recorder(t.query, args)
}

func (t *testClickhouseDriverStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

type testClickhouseDriverTx struct {
}

func (*testClickhouseDriverTx) Commit() error {
	return nil
}

func (*testClickhouseDriverTx) Rollback() error {
	return nil
}
