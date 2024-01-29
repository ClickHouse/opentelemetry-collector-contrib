// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loghouseprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
)

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
