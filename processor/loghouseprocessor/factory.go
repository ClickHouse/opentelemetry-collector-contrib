// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loghouseprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/loghouseprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/metric"
)

const (
	// The value of "type" key in configuration.
	typeStr = "loghouse"
)

// NewFactory creates a factory for the routing processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, component.StabilityLevelBeta),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsProcessor(_ context.Context, params processor.CreateSettings, cfg component.Config, nextConsumer consumer.Logs) (processor.Logs, error) {
	return newLogProcessor(params.TelemetrySettings, cfg, nextConsumer)
}

func newLogProcessor(settings component.TelemetrySettings, config component.Config, nextConsumer consumer.Logs) (*logProcessor, error) {

	meter := settings.MeterProvider.Meter("xoyo-logs")
	nonRoutedLogRecordsCounter, err := meter.Int64Counter(
		"loghouse_observed_logs",
		metric.WithDescription("Number of log records that were not routed to some or all exporters"),
	)
	nonRoutedLogRecordsCounter.Add(context.Background(), 1)
	if err != nil {
		return nil, err
	}

	return &logProcessor{
		logger:                     settings.Logger,
		nonRoutedLogRecordsCounter: nonRoutedLogRecordsCounter,
		nextConsumer:               nextConsumer,
	}, nil
}
