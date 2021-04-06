// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/wasm"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func NewWasmCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	var (
		brokers        []string
		configFile     string
		certFile       string
		keyFile        string
		truststoreFile string
	)

	command := &cobra.Command{
		Use:   "wasm",
		Short: "Deploy and remove inline WASM engine scripts",
	}
	command.AddCommand(wasm.NewGenerateCommand(fs))

	// configure kafka producer
	configClosure := common.FindConfigFile(mgr, &configFile)
	brokersClosure := common.DeduceBrokers(
		fs,
		common.CreateDockerClient,
		configClosure,
		&brokers,
	)
	tlsConfigClosure := common.BuildTLSConfig(&certFile, &keyFile, &truststoreFile)
	adminClosure := common.CreateAdmin(fs, brokersClosure, tlsConfigClosure, configClosure)
	producerClosure := common.CreateProducer(brokersClosure, tlsConfigClosure, configClosure)

	command.AddCommand(
		common.AddKafkaFlags(
			wasm.NewDeployCommand(fs, producerClosure, adminClosure),
			&brokers,
			&configFile,
			&certFile,
			&keyFile,
			&truststoreFile,
		),
	)

	command.AddCommand(
		common.AddKafkaFlags(
			wasm.NewRemoveCommand(producerClosure, adminClosure),
			&brokers,
			&configFile,
			&certFile,
			&keyFile,
			&truststoreFile,
		),
	)

	return command
}
