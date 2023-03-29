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

package clickhouseexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/doug-martin/goqu/v9"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

type logsExporter struct {
	client          *sql.DB
	nativeClient    clickhouse.Conn
	insertSQL       string
	inlineInsertSQL string

	logger *zap.Logger
	cfg    *Config

	wg        *sync.WaitGroup
	closeChan chan struct{}
}

func newLogsExporter(logger *zap.Logger, cfg *Config) (*logsExporter, error) {
	client, nativeClient, err := newClickHouseConn(cfg)
	if err != nil {
		return nil, err
	}

	return &logsExporter{
		client:          client,
		nativeClient:    nativeClient,
		insertSQL:       renderInsertLogsSQL(cfg),
		inlineInsertSQL: renderInlineInsertLogsSQL(cfg),
		logger:          logger,
		cfg:             cfg,
		wg:              new(sync.WaitGroup),
		closeChan:       make(chan struct{}),
	}, nil
}

func (e *logsExporter) start(ctx context.Context, _ component.Host) error {
	if err := createDatabase(ctx, e.cfg); err != nil {
		return err
	}

	if err := createLogsTable(ctx, e.cfg, e.client); err != nil {
		return err
	}
	return nil
}

// shutdown will shut down the exporter.
func (e *logsExporter) shutdown(_ context.Context) error {
	e.wg.Wait()
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *logsExporter) pushNativeLogsData(ctx context.Context, ld plog.Logs) error {
	e.wg.Add(1)
	defer e.wg.Done()
	start := time.Now()
	select {
	case <-e.closeChan:
		return errors.New("shutdown has been called")
	default:
		err := func() error {

			var serviceName string
			resAttr := make(map[string]string)

			values := goqu.Vals{}
			resourceLogs := ld.ResourceLogs()
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
						if r.Body().AsString() != "" {
							logAttr := make(map[string]string, attrs.Len())
							attributesToMap(r.Attributes(), logAttr)

							resMarshal, err := json.Marshal(resAttr)
							if err != nil {
								return err
							}
							logMarshal, err := json.Marshal(logAttr)
							if err != nil {
								return err
							}

							vals := goqu.Vals{r.Timestamp().AsTime(),
								traceutil.TraceIDToHexOrEmptyString(r.TraceID()),
								traceutil.SpanIDToHexOrEmptyString(r.SpanID()),
								uint32(r.Flags()),
								r.SeverityText(),
								int32(r.SeverityNumber()),
								serviceName,
								r.Body().AsString(),
								resMarshal,
								logMarshal}

							values = append(values, vals)
						}

					}
				}

				// clear map for reuse
				for k := range resAttr {
					delete(resAttr, k)
				}
			}

			ds := goqu.Insert(e.cfg.LogsTableName).
				Cols("Timestamp",
					"TraceId",
					"SpanId",
					"TraceFlags",
					"SeverityText",
					"SeverityNumber",
					"ServiceName",
					"Body",
					"ResourceAttributes",
					"LogAttributes").
				Vals(values)
			insertSQL, _, err := ds.ToSQL()
			if err != nil {
				return err
			}
			return e.nativeClient.AsyncInsert(ctx, insertSQL, false)
		}()

		duration := time.Since(start)
		e.logger.Info("insert logs", zap.Int("records", ld.LogRecordCount()),
			zap.String("cost", duration.String()))
		return err
	}
}

func formatInsert(insertValuesArray []string, inlineInsertSQL string) string {
	valuesString := strings.Join(insertValuesArray, ",")
	formattedInsertQuery := inlineInsertSQL + valuesString + " SETTINGS async_insert=1, wait_for_async_insert=0"
	return formattedInsertQuery
}

func prepareValues(r plog.LogRecord, serviceName string, resAttr map[string]string, logAttr map[string]string) (string, error) {
	resAttrString, err := json.Marshal(resAttr)
	if err != nil {
		return "", err
	}
	logAttrString, err := json.Marshal(logAttr)
	if err != nil {
		return "", err
	}

	values := fmt.Sprintf(`(%s, %s, %s, %d, %s, %d, %s, %s, %s, %s)`, r.Timestamp().AsTime().Format(time.UnixDate),
		traceutil.TraceIDToHexOrEmptyString(r.TraceID()),
		traceutil.SpanIDToHexOrEmptyString(r.SpanID()),
		uint32(r.Flags()),
		r.SeverityText(),
		int32(r.SeverityNumber()),
		serviceName,
		r.Body().AsString(),
		string(resAttrString),
		string(logAttrString))

	return values, nil
}

func (e *logsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	err := func() error {
		scope, err := e.client.Begin()
		if err != nil {
			return fmt.Errorf("Begin:%w", err)
		}
		batch, err := scope.Prepare(e.insertSQL)
		if err != nil {
			return fmt.Errorf("Prepare:%w", err)
		}

		var serviceName string
		resAttr := make(map[string]string)

		resourceLogs := ld.ResourceLogs()
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

					_, err = batch.Exec(
						r.Timestamp().AsTime(),
						traceutil.TraceIDToHexOrEmptyString(r.TraceID()),
						traceutil.SpanIDToHexOrEmptyString(r.SpanID()),
						uint32(r.Flags()),
						r.SeverityText(),
						int32(r.SeverityNumber()),
						serviceName,
						r.Body().AsString(),
						resAttr,
						logAttr,
					)
					if err != nil {
						return fmt.Errorf("Append:%w", err)
					}
				}
			}

			// clear map for reuse
			for k := range resAttr {
				delete(resAttr, k)
			}
		}

		return scope.Commit()
	}()

	duration := time.Since(start)
	e.logger.Info("insert logs", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))
	return err
}

func attributesToMap(attributes pcommon.Map, dest map[string]string) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		dest[k] = v.AsString()
		return true
	})
}

const (
	// language=ClickHouse SQL
	createLogsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
     Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
     TraceId String CODEC(ZSTD(1)),
     SpanId String CODEC(ZSTD(1)),
     TraceFlags UInt32 CODEC(ZSTD(1)),
     SeverityText LowCardinality(String) CODEC(ZSTD(1)),
     SeverityNumber Int32 CODEC(ZSTD(1)),
     ServiceName LowCardinality(String) CODEC(ZSTD(1)),
     Body String CODEC(ZSTD(1)),
     ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
     LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
     INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
     INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
     INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
) ENGINE MergeTree()
%s
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SeverityText, toUnixTimestamp(Timestamp), TraceId)
SETTINGS index_granularity=8192, ttl_only_drop_parts = 1;
`

	// language=ClickHouse SQL
	insertLogsSQLTemplate = `INSERT INTO %s (
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
                        )`
	inlineinsertLogsSQLTemplate = `INSERT INTO %s (
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
                        ) SETTINGS async_insert=1, wait_for_async_insert=0 VALUES(
                                 ?,
                                 ?,
                                 ?,
                                 ?,
                                 ?,
                                 ?,
                                 ?,
                                 ?,
                                 ?,
                                 ?)`
)

var driverName = "clickhouse" // for testing

// newClickHouseClient create a clickhouse client.
// used by metrics and traces:
func newClickHouseClient(cfg *Config) (*sql.DB, error) {
	db, err := cfg.buildDB(cfg.Database)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// used by logs:
func newClickHouseConn(cfg *Config) (*sql.DB, driver.Conn, error) {
	endpoint := cfg.Endpoint

	if len(cfg.ConnectionParams) > 0 {
		values := make(url.Values, len(cfg.ConnectionParams))
		for k, v := range cfg.ConnectionParams {
			values.Add(k, v)
		}

		if !strings.Contains(endpoint, "?") {
			endpoint += "?"
		} else if !strings.HasSuffix(endpoint, "&") {
			endpoint += "&"
		}
		values.Add("debug", "true")
		endpoint += values.Encode()
	}

	opts, err := clickhouse.ParseDSN(endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse endpoint: %w", err)
	}
	// TODO config
	opts.Settings["async_insert"] = 1
	opts.Settings["wait_for_async_insert"] = 0
	opts.Debug = true

	opts.Auth = clickhouse.Auth{
		Database: cfg.Database,
		Username: cfg.Username,
		Password: cfg.Password,
	}

	// can return a "bad" connection if misconfigured, we won't know
	// until a Ping, Exec, etc.. is done
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, nil, err
	}
	return clickhouse.OpenDB(opts), conn, nil
}

func createDatabase(ctx context.Context, cfg *Config) error {
	// use default database to create new database
	if cfg.Database == defaultDatabase {
		return nil
	}

	db, err := cfg.buildDB(defaultDatabase)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.Database)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create database:%w", err)
	}
	return nil
}

func createLogsTable(ctx context.Context, cfg *Config, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, renderCreateLogsTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create logs table sql: %w", err)
	}
	return nil
}

func renderCreateLogsTableSQL(cfg *Config) string {
	var ttlExpr string
	if cfg.TTLDays > 0 {
		ttlExpr = fmt.Sprintf(`TTL toDateTime(Timestamp) + toIntervalDay(%d)`, cfg.TTLDays)
	}
	return fmt.Sprintf(createLogsTableSQL, cfg.LogsTableName, ttlExpr)
}

func renderInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(insertLogsSQLTemplate, cfg.LogsTableName)
}

func renderInlineInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(inlineinsertLogsSQLTemplate, cfg.LogsTableName)
}
