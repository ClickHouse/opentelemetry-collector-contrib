// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loghouseprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
)

func Test_dp(t *testing.T) {
	t.Run("json severity", func(t *testing.T) {
		// Define the test cases in a table format
		tests := []struct {
			name          string
			input         string
			expectedText  string
			expectedLevel plog.SeverityNumber
		}{
			{"Error severity", "{\"level\": \"fatal\"}", FATAL, plog.SeverityNumberFatal},
			{"Error severity", "{\"level\": \"error\"}", ERROR, plog.SeverityNumberError},
			{"Warning severity", "{\"level\": \"warn\"}", WARN, plog.SeverityNumberWarn},
			{"Info severity", "{\"level\": \"info\"}", INFO, plog.SeverityNumberInfo},
			{"Debug severity", "{\"level\": \"debug\"}", DEBUG, plog.SeverityNumberDebug},
			{"Debug severity", "{\"level\": \"trace\"}", TRACE, plog.SeverityNumberTrace},
			// Add more severity levels if needed
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				log := plog.NewLogRecord()
				log.Body().SetStr(tc.input)

				err := processOneLogLine(&log)
				assert.NoError(t, err)

				assert.Equal(t, tc.expectedText, log.SeverityText())
				assert.Equal(t, tc.expectedLevel, log.SeverityNumber())
			})
		}
	})
}

func Test_plaintextSeverity(t *testing.T) {
	t.Run("trace", func(t *testing.T) {
		line := "2023-01-02 11:20:30 <TRACE> some log line"
		log := plog.NewLogRecord()
		log.Body().SetStr(line)

		parsePlaintextSeverity(&log)

		assert.Equal(t, TRACE, log.SeverityText())
		assert.Equal(t, plog.SeverityNumberTrace, log.SeverityNumber())
	})
	t.Run("information", func(t *testing.T) {
		line := "2023-01-02 11:20:30 <INFORMATION> some log line"
		log := plog.NewLogRecord()
		log.Body().SetStr(line)

		parsePlaintextSeverity(&log)

		assert.Equal(t, INFO, log.SeverityText())
		assert.Equal(t, plog.SeverityNumberInfo, log.SeverityNumber())
	})
	t.Run("case insensitive", func(t *testing.T) {
		line := "2023-01-02 11:20:30 <wArN> some log line"
		log := plog.NewLogRecord()
		log.Body().SetStr(line)

		parsePlaintextSeverity(&log)

		assert.Equal(t, WARN, log.SeverityText())
		assert.Equal(t, plog.SeverityNumberWarn, log.SeverityNumber())
	})
}

func Test_chSeverity(t *testing.T) {
	type testCase struct {
		in    string
		exStr string
		exNum plog.SeverityNumber
	}
	testCases := []testCase{
		{
			in:    "Info",
			exStr: INFO,
			exNum: plog.SeverityNumberInfo,
		},
		{
			in:    "inforMation",
			exStr: INFO,
			exNum: plog.SeverityNumberInfo,
		},
		{
			in:    "warn",
			exStr: WARN,
			exNum: plog.SeverityNumberWarn,
		},
		{
			in:    "warnIng",
			exStr: WARN,
			exNum: plog.SeverityNumberWarn,
		},
		{
			in:    "debug",
			exStr: DEBUG,
			exNum: plog.SeverityNumberDebug,
		},
		{
			in:    "tracE",
			exStr: TRACE,
			exNum: plog.SeverityNumberTrace,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			line := plog.NewLogRecord()
			line.Attributes().PutStr("level", tc.in)

			ok := parseCHSeverity(&line)
			assert.True(t, ok)

			assert.Equal(t, tc.exStr, line.SeverityText())
			assert.Equal(t, tc.exNum, line.SeverityNumber())
		})
	}

}

func Test_trimK8sPreamble(t *testing.T) {
	type testCase struct {
		in    string
		exOut string
		exOk  bool
	}
	testCases := []testCase{
		{
			in:    "1234",
			exOut: "1234",
			exOk:  false,
		},
		{
			in:    "}}}}}",
			exOut: "}}}}}",
			exOk:  false,
		},
		{
			in:    "",
			exOut: "",
			exOk:  false,
		},
		{
			in:    "junk {BLAH} blah",
			exOut: "junk {BLAH} blah",
			exOk:  false,
			// we ignore the {} where it just exists within the string, as it doesnt look like a valid json log line
		},
		{
			in:    "junk {BLAH}",
			exOut: "{BLAH}",
			exOk:  true,
			// where there is some preamble before a json like string, we trim the preamble.
		},
		{
			in:    "junk {} something else",
			exOut: "junk {} something else",
			exOk:  false,
			// we ignore the {} where it just exists within the string, as it doesnt look like a valid json log line
		},
	}
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			out, ok := trimK8sLogPreamble(tc.in)
			assert.Equal(t, tc.exOk, ok)
			assert.Equal(t, tc.exOut, out)
		})
	}
}

func Test_timestamp(t *testing.T) {

	t.Run("prefer ClickHouse timestamp to K8s", func(t *testing.T) {
		line := "2024-01-29T13:27:10.952171171Z stderr F 2024.01.29 13:27:10.952127 [ 841 ] {} <Trace> WriteBufferFromS3: finalizeImpl WriteBufferFromS3. Details: bucket red, key."
		log := plog.NewLogRecord()
		log.Body().SetStr(line)

		err := parsePlaintextTimestamp(&log)
		assert.NoError(t, err)

		assert.Equal(t, "2024-01-29 13:27:10.952127 +0000 UTC", log.Timestamp().String())

	})
	t.Run("fallback to K8s timestamp if ClickHouse doesn't work", func(t *testing.T) {
		line := "2024-01-29T13:27:10.952171171Z stderr F 2024.0BLAH1.29 13:27:10.952127 [ 841 ] {} <Trace> WriteBufferFromS3: finalizeImpl WriteBufferFromS3. Details:"
		log := plog.NewLogRecord()
		log.Body().SetStr(line)

		err := parsePlaintextTimestamp(&log)
		assert.NoError(t, err)

		assert.Equal(t, "2024-01-29 13:27:10.952171171 +0000 UTC", log.Timestamp().String())

	})
}

func Test_processLine(t *testing.T) {
	t.Run("server", func(t *testing.T) {
		line := "{\"date_time\":\"1701792375.853698\",\"thread_name\":\"\",\"thread_id\":\"4751767\",\"level\":\"8\",\"query_id\":\"\",\"logger_name\":\"SystemLog (system.asynchronous_metric_log)\",\"message\":\"Flushed system log up to offset 532\",\"source_file\":\"src\\/Interpreters\\/SystemLog.cpp; void DB::SystemLog<DB::AsynchronousMetricLogElement>::flushImpl(const std::vector<LogElement> &, uint64_t) [LogElement = DB::AsynchronousMetricLogElement]\",\"source_line\":\"529\"}\n"
		log := plog.NewLogRecord()
		log.Body().SetStr(line)
		log.Attributes().PutStr("k8s.container.name", "potter-server")

		err := processOneLogLine(&log)
		assert.NoError(t, err)

		assert.Equal(t, "TRACE", log.SeverityText())
		assert.Equal(t, plog.SeverityNumberTrace, log.SeverityNumber())
	})

	t.Run("keeper", func(t *testing.T) {
		line := "{\"date_time\":\"1701792375.853698\",\"thread_name\":\"\",\"thread_id\":\"4751767\",\"level\":\"8\",\"query_id\":\"\",\"logger_name\":\"SystemLog (system.asynchronous_metric_log)\",\"message\":\"Flushed system log up to offset 532\",\"source_file\":\"src\\/Interpreters\\/SystemLog.cpp; void DB::SystemLog<DB::AsynchronousMetricLogElement>::flushImpl(const std::vector<LogElement> &, uint64_t) [LogElement = DB::AsynchronousMetricLogElement]\",\"source_line\":\"529\"}\n"
		log := plog.NewLogRecord()
		log.Body().SetStr(line)
		log.Attributes().PutStr("k8s.container.name", "potter-keeper")

		err := processOneLogLine(&log)
		assert.NoError(t, err)

		assert.Equal(t, "TRACE", log.SeverityText())
		assert.Equal(t, plog.SeverityNumberTrace, log.SeverityNumber())
	})

	t.Run("generic", func(t *testing.T) {
		line := "{}"
		log := plog.NewLogRecord()
		log.Body().SetStr(line)
		log.Attributes().PutStr("k8s.container.name", "operator")

		err := processOneLogLine(&log)
		assert.NoError(t, err)

	})
}

// func assertAttr(t *testing.T, expected, key string, log *plog.LogRecord) {
// 	val, ok := log.Attributes().Get(key)
// 	assert.True(t, ok)
// 	assert.Equal(t, expected, val.Str())
// }
//  TODO for testing that metrics get updated
// func getCounter() metric.Int64Counter {
// 	meterProvider := sdkmetric.NewMeterProvider()
//
// 	meter := meterProvider.Meter("xoyo-logs")
// 	observedLogsCtr, err := meter.Int64Counter(
// 		"loghouse_observed_logs",
// 		metric.WithDescription("Number of log records that were not routed to some or all exporters"),
// 	)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return observedLogsCtr
//
// }
