// Code generated by mdatagen. DO NOT EDIT.

package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	Type      = component.MustNewType("otlpjson")
	ScopeName = "github.com/open-telemetry/opentelemetry-collector-contrib/connector/otlpjsonconnector"
)

const (
	LogsToMetricsStability = component.StabilityLevelAlpha
	LogsToTracesStability  = component.StabilityLevelAlpha
	LogsToLogsStability    = component.StabilityLevelAlpha
)
