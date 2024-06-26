// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package podmanreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, "podman_stats", string(factory.Type()))

	config := factory.CreateDefaultConfig()
	assert.NotNil(t, config, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(config))
}

func TestCreateReceiver(t *testing.T) {
	factory := NewFactory()
	config := factory.CreateDefaultConfig()

	params := receivertest.NewNopCreateSettings()
	traceReceiver, err := factory.CreateTracesReceiver(context.Background(), params, config, consumertest.NewNop())
	assert.ErrorIs(t, err, component.ErrDataTypeIsNotSupported)
	assert.Nil(t, traceReceiver)

	metricReceiver, err := factory.CreateMetricsReceiver(context.Background(), params, config, consumertest.NewNop())
	assert.NoError(t, err, "Metric receiver creation failed")
	assert.NotNil(t, metricReceiver, "Receiver creation failed")
}

func TestCreateInvalidEndpoint(t *testing.T) {
	factory := NewFactory()
	config := factory.CreateDefaultConfig()
	receiverCfg := config.(*Config)

	receiverCfg.Endpoint = ""

	params := receivertest.NewNopCreateSettings()
	recv, err := factory.CreateMetricsReceiver(context.Background(), params, receiverCfg, consumertest.NewNop())
	assert.Nil(t, recv)
	assert.Error(t, err)
	assert.Equal(t, "config.Endpoint must be specified", err.Error())
}
