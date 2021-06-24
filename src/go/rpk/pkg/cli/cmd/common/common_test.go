// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package common

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	ccommon "github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/container/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func TestDeduceBrokers(t *testing.T) {
	tests := []struct {
		name     string
		client   func() (ccommon.Client, error)
		config   func() (*config.Config, error)
		brokers  []string
		before   func()
		cleanup  func()
		expected []string
	}{{
		name: "it should prioritize the flag over the config & containers",
		client: func() (ccommon.Client, error) {
			return &ccommon.MockClient{
				MockContainerInspect: ccommon.MockContainerInspect,
				MockContainerList: func(
					_ context.Context,
					_ types.ContainerListOptions,
				) ([]types.Container, error) {
					return []types.Container{{
						ID: "a",
						Labels: map[string]string{
							"node-id": "0",
						},
					}}, nil
				},
			}, nil
		},
		brokers:  []string{"192.168.34.12:9093"},
		expected: []string{"192.168.34.12:9093"},
	}, {
		name: "it should take the value from the env vars if the flag wasn't passed",
		client: func() (ccommon.Client, error) {
			return &ccommon.MockClient{
				MockContainerInspect: ccommon.MockContainerInspect,
				MockContainerList: func(
					_ context.Context,
					_ types.ContainerListOptions,
				) ([]types.Container, error) {
					return []types.Container{{
						ID: "a",
						Labels: map[string]string{
							"node-id": "0",
						},
					}}, nil
				},
			}, nil
		},
		before: func() {
			os.Setenv("REDPANDA_BROKERS", "192.168.34.12:9093,123.4.5.78:9092")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_BROKERS")
		},
		expected: []string{"192.168.34.12:9093", "123.4.5.78:9092"},
	}, {
		name: "it should prioritize the local containers over the config",
		client: func() (ccommon.Client, error) {
			return &ccommon.MockClient{
				MockContainerInspect: ccommon.MockContainerInspect,
				MockContainerList: func(
					_ context.Context,
					_ types.ContainerListOptions,
				) ([]types.Container, error) {
					return []types.Container{{
						ID: "a",
						Labels: map[string]string{
							"node-id": "0",
						},
					}}, nil
				},
			}, nil
		},
		expected: []string{"127.0.0.1:89080"},
	}, {
		name: "it should fall back to the config if the docker client" +
			" can't be init'd",
		client: func() (ccommon.Client, error) {
			return nil, errors.New("The docker client can't be initialized")
		},
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Rpk.KafkaApi.Brokers = []string{"192.168.25.88:1235"}
			return conf, nil
		},
		expected: []string{"192.168.25.88:1235"},
	}, {
		name: "it should fall back to the default addr if there's an" +
			" error reading the config",
		config: func() (*config.Config, error) {
			return nil, errors.New("The config file couldn't be read")
		},
		expected: []string{"127.0.0.1:9092"},
	}, {
		name: "it should prioritize the config over the default broker addr",
		config: func() (*config.Config, error) {
			conf := config.Default()
			conf.Rpk.KafkaApi.Brokers = []string{"192.168.25.87:1234", "192.168.26.98:9092"}
			return conf, nil
		},
		expected: []string{"192.168.25.87:1234", "192.168.26.98:9092"},
	}, {
		name:     "it should return 127.0.0.1:9092 if no config sources yield a brokers list",
		expected: []string{"127.0.0.1:9092"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if tt.before != nil {
				tt.before()
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}
			client := func() (ccommon.Client, error) {
				return &ccommon.MockClient{}, nil
			}
			config := func() (*config.Config, error) {
				return config.Default(), nil
			}
			brokersList := []string{}
			brokers := &brokersList

			if tt.client != nil {
				client = tt.client
			}
			if tt.config != nil {
				config = tt.config
			}
			if tt.brokers != nil {
				brokers = &tt.brokers
			}
			bs := DeduceBrokers(client, config, brokers)()
			require.Exactly(st, tt.expected, bs)
		})
	}
}

func TestAddKafkaFlags(t *testing.T) {
	var (
		brokers        []string
		configFile     string
		user           string
		password       string
		mechanism      string
		certFile       string
		keyFile        string
		truststoreFile string
	)
	command := func() *cobra.Command {
		parent := &cobra.Command{
			Use: "parent",
			RunE: func(_ *cobra.Command, _ []string) error {
				return nil
			},
		}
		child := &cobra.Command{
			Use: "child",
			RunE: func(_ *cobra.Command, _ []string) error {
				return nil
			},
		}
		parent.AddCommand(child)

		enableTLS := false

		AddKafkaFlags(
			parent,
			&configFile,
			&user,
			&password,
			&mechanism,
			&enableTLS,
			&certFile,
			&keyFile,
			&truststoreFile,
			&brokers,
		)
		return parent
	}

	cmd := command()
	cmd.SetArgs([]string{
		"--config", "arbitraryconfig.yaml",
		"--brokers", "192.168.72.22:9092,localhost:9092",
		"--user", "david",
		"--password", "verysecrethaha",
		"--sasl-mechanism", "some-mechanism",
		"--tls-cert", "cert.pem",
		"--tls-key", "key.pem",
		"--tls-truststore", "truststore.pem",
	})

	err := cmd.Execute()
	require.NoError(t, err)

	require.Exactly(t, "arbitraryconfig.yaml", configFile)
	require.Exactly(t, []string{"192.168.72.22:9092", "localhost:9092"}, brokers)
	require.Exactly(t, "david", user)
	require.Exactly(t, "verysecrethaha", password)
	require.Exactly(t, "some-mechanism", mechanism)
	require.Exactly(t, "cert.pem", certFile)
	require.Exactly(t, "key.pem", keyFile)
	require.Exactly(t, "truststore.pem", truststoreFile)

	// The flags should be available for the children commands too
	cmd = command() // reset it.
	cmd.SetArgs([]string{
		"child", // so that it executes the child command
		"--config", "justaconfig.yaml",
		"--brokers", "192.168.72.23:9092",
		"--brokers", "mykafkahost:9093",
		"--user", "juan",
		"--password", "sosecure",
		"--sasl-mechanism", "whatevs",
		"--tls-cert", "cert1.pem",
		"--tls-key", "key1.pem",
		"--tls-truststore", "truststore1.pem",
	})

	err = cmd.Execute()
	require.NoError(t, err)

	require.Exactly(t, "justaconfig.yaml", configFile)
	require.Exactly(t, []string{"192.168.72.23:9092", "mykafkahost:9093"}, brokers)
	require.Exactly(t, "juan", user)
	require.Exactly(t, "sosecure", password)
	require.Exactly(t, "whatevs", mechanism)
	require.Exactly(t, "cert1.pem", certFile)
	require.Exactly(t, "key1.pem", keyFile)
	require.Exactly(t, "truststore1.pem", truststoreFile)
}

func TestKafkaAuthConfig(t *testing.T) {
	tests := []struct {
		name           string
		user           string
		password       string
		mechanism      string
		before         func()
		cleanup        func()
		expected       *config.SASL
		expectedErrMsg string
	}{{
		name:           "it should fail if user is empty",
		password:       "somethingsecure",
		expectedErrMsg: "empty user. Pass --user to set a value.",
	}, {
		name:           "it should fail if password is empty",
		user:           "usuario",
		expectedErrMsg: "empty password. Pass --password to set a value.",
	}, {
		name:           "it should fail if both user and password are empty",
		expectedErrMsg: ErrNoCredentials.Error(),
	}, {
		name:      "it should fail if the mechanism isn't supported",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "super-crypto-3000",
		expectedErrMsg: "unsupported mechanism 'super-crypto-3000'. Pass --sasl-mechanism to set a value." +
			" Supported: SCRAM-SHA-256, SCRAM-SHA-512.",
	}, {
		name:      "it should support SCRAM-SHA-256",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-256",
		expected:  &config.SASL{User: "usuario", Password: "contraseño", Mechanism: "SCRAM-SHA-256"},
	}, {
		name:      "it should support SCRAM-SHA-512",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-512",
		expected:  &config.SASL{User: "usuario", Password: "contraseño", Mechanism: "SCRAM-SHA-512"},
	}, {
		name: "it should pick up the values from env vars if the vars' values is empty",
		before: func() {
			os.Setenv("REDPANDA_SASL_USERNAME", "ringo")
			os.Setenv("REDPANDA_SASL_PASSWORD", "octopussgarden66")
			os.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-512")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_SASL_USERNAME")
			os.Unsetenv("REDPANDA_SASL_PASSWORD")
			os.Unsetenv("REDPANDA_SASL_MECHANISM")
		},
		expected: &config.SASL{User: "ringo", Password: "octopussgarden66", Mechanism: "SCRAM-SHA-512"},
	}, {
		name:      "it should give priority to values set through the flags",
		user:      "usuario",
		password:  "contraseño",
		mechanism: "SCRAM-SHA-512",
		before: func() {
			os.Setenv("REDPANDA_SASL_USERNAME", "ringo")
			os.Setenv("REDPANDA_SASL_PASSWORD", "octopussgarden66")
			os.Setenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-512")
		},
		cleanup: func() {
			os.Unsetenv("REDPANDA_SASL_USERNAME")
			os.Unsetenv("REDPANDA_SASL_PASSWORD")
			os.Unsetenv("REDPANDA_SASL_MECHANISM")
		},
		// Disregards the env vars' values
		expected: &config.SASL{User: "usuario", Password: "contraseño", Mechanism: "SCRAM-SHA-512"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if tt.before != nil {
				tt.before()
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}
			closure := KafkaAuthConfig(&tt.user, &tt.password, &tt.mechanism)
			res, err := closure()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.Exactly(st, tt.expected, res)
		})
	}
}

func TestBuildTLSConfig(t *testing.T) {
	truststoreContents := `-----BEGIN CERTIFICATE-----
MIIDDjCCAfagAwIBAgIUZHm6D1aJYtmVEV1X23m+oK9EJgcwDQYJKoZIhvcNAQEL
BQAwLTETMBEGA1UECgwKVmVjdG9yaXplZDEWMBQGA1UEAwwNVmVjdG9yaXplZCBD
QTAeFw0yMTA0MDYwMjMzMjNaFw0yMjA0MDYwMjMzMjNaMC0xEzARBgNVBAoMClZl
Y3Rvcml6ZWQxFjAUBgNVBAMMDVZlY3Rvcml6ZWQgQ0EwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDZN7ytDxWGvxprVvydv0hwuD3hGcZFbJaeDXrIInGd
RGK8zr/zoq3oAgw1ZW0OabID3OKs9tezKGM5wPUvzGfU94qhi1ot040Utw+Jf2tQ
GA3T1X+VHRTUlqDVVnIvhcAiy21bMUMVuFl4QtJnWx9ZljkCFo8IIh/3Sq88ORDl
gzfM3cK1kk+60uKzXNvgK8ShrZ0GYsbmxncxhbdr2O7mSdVcO6x0tbmNDLc5Hx+w
34z2hRHReavw3KDFfaIdHZE2tSQ4xh25F9aZQiPsTaGzPtGozLl7Ck1p9Ew4d+MO
EXd51gGeIdP6EUD8exOMfRq2B6/p18HyjMsuKQKhjx2NAgMBAAGjJjAkMA4GA1Ud
DwEB/wQEAwIC5DASBgNVHRMBAf8ECDAGAQH/AgEBMA0GCSqGSIb3DQEBCwUAA4IB
AQCse+GLdlBi77fOgDgX8dby2PVZiSoW8cE1uRJzaXvw9mrz83RW2SxnW3och6Mm
cixyv+taomZfYNM2wbOzEpkI0QEcV9CF/9Sx5RQVKsoAQkjkmzpHUeRp5ha/VAYq
KWVCj9Ej+1y8dE0+AvltyymRbcMKUSk3vXK6HmzCGn2XAVz1WBQHQHXe0vwQGAXu
Tq7VoNh+LNEud7ro5Hwi5aiDA1B7HZum6u2MvU5KwGY3txDve1Jn0bWOa8J3HjIN
wHAv/4PAPweXflPAVHkUR4VOslBdZo/tAcSbG/Zr3cneBt7VYnPyO+IAe7S54u9g
eKlljKHOgHw7gXOzsvWTVQbm
-----END CERTIFICATE-----`

	certContents := `Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number: 1 (0x1)
        Signature Algorithm: sha256WithRSAEncryption
        Issuer: O=Vectorized, CN=Vectorized CA
        Validity
            Not Before: Apr  6 02:33:23 2021 GMT
            Not After : Apr  6 02:33:23 2022 GMT
        Subject: O=Vectorized
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
                RSA Public-Key: (2048 bit)
                Modulus:
                    00:d5:38:bc:86:a5:49:83:9b:d0:eb:a2:d9:96:83:
                    8c:3c:56:d3:be:4b:d9:ea:d2:be:96:60:ed:05:b1:
                    a7:95:93:98:35:a0:96:cc:bf:b1:7c:63:d1:ab:9b:
                    d4:70:ee:e9:dc:99:3c:54:88:39:ce:13:ba:78:cc:
                    fb:46:94:ed:a2:d1:ed:a2:bb:93:db:02:c6:ab:3c:
                    15:d4:71:e1:5c:00:43:f1:11:b8:9c:f3:ac:eb:70:
                    e6:a6:ff:5a:bd:f8:e4:ae:d5:99:0a:8e:de:77:c8:
                    8b:17:38:9a:2a:60:b2:c5:96:00:55:0e:0a:fc:f7:
                    84:0a:1d:a8:20:ca:c2:bb:10:4b:78:a7:9c:6e:8b:
                    51:41:03:83:27:92:a6:3a:29:4a:e7:bb:96:e1:c2:
                    68:72:f4:2b:03:4f:fc:09:73:2c:d8:71:4c:93:fc:
                    32:a2:83:80:55:fc:c3:ca:17:c2:52:1c:9b:33:94:
                    3b:64:4f:ab:4f:0e:76:ca:5e:e6:0f:6c:7e:18:8e:
                    a3:4b:b3:46:f3:e2:c7:b9:eb:83:87:ba:a7:ec:d2:
                    02:75:29:e8:14:c6:c5:22:f8:78:74:50:0d:36:ed:
                    5c:6b:00:bf:56:68:9d:b0:e9:01:0c:21:4a:0b:33:
                    ca:9c:86:d9:87:86:91:a9:9d:1f:c7:12:ea:13:b2:
                    eb:27
                Exponent: 65537 (0x10001)
        X509v3 extensions:
            X509v3 Key Usage: critical
                Digital Signature, Key Encipherment
            X509v3 Extended Key Usage: 
                TLS Web Server Authentication, TLS Web Client Authentication
            X509v3 Subject Alternative Name: critical
                DNS:localhost, IP Address:127.0.0.1
    Signature Algorithm: sha256WithRSAEncryption
         15:5a:32:5d:78:53:25:0f:53:0f:21:62:d4:6e:b0:41:3c:43:
         f8:ed:d8:b5:49:c9:7f:f4:39:04:24:4c:9a:62:19:a0:5f:6e:
         c4:fb:5c:72:2e:22:01:3b:10:98:24:cc:80:53:f4:58:a0:2e:
         f9:ec:ca:77:63:0b:eb:51:e7:15:e3:35:a3:33:2b:99:33:a5:
         3d:bd:04:df:af:db:0a:51:52:1b:e5:74:9e:27:ad:5c:56:94:
         c2:12:f0:02:a3:4c:85:27:91:f5:12:1c:80:e7:fa:1d:96:08:
         5d:ca:c2:be:b3:6a:50:e4:1b:5b:53:2f:10:ed:26:86:73:82:
         a4:c1:39:1a:33:90:69:a3:e4:ed:f5:41:22:0a:cb:d6:35:d8:
         33:2a:be:4b:ef:51:4a:d3:03:85:7c:68:d2:3a:a6:45:30:85:
         1e:7a:96:7d:a2:95:89:a8:71:c7:72:3b:f5:f1:c1:ed:95:ab:
         89:41:95:77:a1:84:64:fb:b4:c7:74:e4:55:a2:2e:3c:31:bb:
         55:33:12:59:c2:4e:96:b2:fb:2e:2c:9d:0a:51:0d:fe:3a:24:
         c6:77:52:45:ba:e8:63:42:66:e9:48:98:85:3d:91:a9:56:3a:
         e3:c4:dd:b8:78:6e:cc:8c:21:97:dc:b3:87:b1:1c:e5:74:83:
         61:1e:9c:b9
-----BEGIN CERTIFICATE-----
MIIDDTCCAfWgAwIBAgIBATANBgkqhkiG9w0BAQsFADAtMRMwEQYDVQQKDApWZWN0
b3JpemVkMRYwFAYDVQQDDA1WZWN0b3JpemVkIENBMB4XDTIxMDQwNjAyMzMyM1oX
DTIyMDQwNjAyMzMyM1owFTETMBEGA1UECgwKVmVjdG9yaXplZDCCASIwDQYJKoZI
hvcNAQEBBQADggEPADCCAQoCggEBANU4vIalSYOb0Oui2ZaDjDxW075L2erSvpZg
7QWxp5WTmDWglsy/sXxj0aub1HDu6dyZPFSIOc4TunjM+0aU7aLR7aK7k9sCxqs8
FdRx4VwAQ/ERuJzzrOtw5qb/Wr345K7VmQqO3nfIixc4mipgssWWAFUOCvz3hAod
qCDKwrsQS3innG6LUUEDgyeSpjopSue7luHCaHL0KwNP/AlzLNhxTJP8MqKDgFX8
w8oXwlIcmzOUO2RPq08Odspe5g9sfhiOo0uzRvPix7nrg4e6p+zSAnUp6BTGxSL4
eHRQDTbtXGsAv1ZonbDpAQwhSgszypyG2YeGkamdH8cS6hOy6ycCAwEAAaNQME4w
DgYDVR0PAQH/BAQDAgWgMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjAd
BgNVHREBAf8EEzARgglsb2NhbGhvc3SHBH8AAAEwDQYJKoZIhvcNAQELBQADggEB
ABVaMl14UyUPUw8hYtRusEE8Q/jt2LVJyX/0OQQkTJpiGaBfbsT7XHIuIgE7EJgk
zIBT9FigLvnsyndjC+tR5xXjNaMzK5kzpT29BN+v2wpRUhvldJ4nrVxWlMIS8AKj
TIUnkfUSHIDn+h2WCF3Kwr6zalDkG1tTLxDtJoZzgqTBORozkGmj5O31QSIKy9Y1
2DMqvkvvUUrTA4V8aNI6pkUwhR56ln2ilYmoccdyO/Xxwe2Vq4lBlXehhGT7tMd0
5FWiLjwxu1UzElnCTpay+y4snQpRDf46JMZ3UkW66GNCZulImIU9kalWOuPE3bh4
bsyMIZfcs4exHOV0g2EenLk=
-----END CERTIFICATE-----`

	keyContents := `-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEA1Ti8hqVJg5vQ66LZloOMPFbTvkvZ6tK+lmDtBbGnlZOYNaCW
zL+xfGPRq5vUcO7p3Jk8VIg5zhO6eMz7RpTtotHtoruT2wLGqzwV1HHhXABD8RG4
nPOs63Dmpv9avfjkrtWZCo7ed8iLFziaKmCyxZYAVQ4K/PeECh2oIMrCuxBLeKec
botRQQODJ5KmOilK57uW4cJocvQrA0/8CXMs2HFMk/wyooOAVfzDyhfCUhybM5Q7
ZE+rTw52yl7mD2x+GI6jS7NG8+LHueuDh7qn7NICdSnoFMbFIvh4dFANNu1cawC/
VmidsOkBDCFKCzPKnIbZh4aRqZ0fxxLqE7LrJwIDAQABAoIBAQDQ3yuPmwtQ6arX
qkgMsgEGeugiWpu29YvONFT8ZvQMCvHoVtBi8sYjXIVg3t5VYzWk7Fe1V12JCrp4
7BSbJ/lCrvNjnu1Qdn+37rxTyNtDDN+BoCKBXhPe8FKC9VMnFlKvEn9BYIN+Q+49
aS1cpi16cV8R8xfAh5fJcRPqS7ZHF/1rhUqkcoV956h5RtQy3k/BIVpJydbrb17U
xEhiCFyRbCIEEnX2hTRFrRt4hEIwa2YceWMsY8ddCngtS/i4UGLsaYLCMrTgtvwS
H9uI1q0gL5tcp7qo4MQo9BRtbiazV70MofabF19tapDlbPoqcggN/sTi4VQzFWsx
YKShZhIBAoGBAPA5ZaH6pg76O+E47kPbugx2V6y8YongvBOe+MXkwXG3samLUhN9
63th3qhDOqqwUN8FgJYqATbn6lsvVnIhoJ1EK126grVeZme7FCTswjHKd2gaVAaz
GBJ7ejMMoHzw1IGSXxnZ69XFN4xUSAiCeq58LWl3PJmtpt+vZ2ZZrA+HAoGBAOM5
YJP7YzGPG/OX4EVttGzcMCigTUTKZwH4aNTzJm2KDHiS3uiW6XMgMsY8H057XZbT
QjB7QnokRUuVqD/Zugh2bHZcHiJNT9re/TZwqXPXh2B4fY0ZOSAImMpsXWdfJSKK
anIroNF4v5gw+4zqtEBE8Zc7Ct+gbvfPp9qdbu9hAoGAXkAWyQufdYbmUYJVsVgX
UeZolcQ/4RrEj+oybupGn4hT81JPPIiOCJWol1nxPaD5ydbN0ZzfZxxszaPwBc19
x9ZEMX0I5YIJKa+zwp0FwCVQ3g5eY1aHHlFF65uLqBmRNtkn6OugZPoAxlUXAge3
fJgJ9TQsGZuROngGWJjcMicCgYEAxPd12pFt2QX+6tfalxST9FGihXT/xgPV6wVU
ilQEGawzR0m5ZNF8qEle+iwfzz5tUFLs623NoGdUkkK2yDKKas+NEcSkcoOmF0p5
IPnkSgCo311TKD6XIEeTetUY2oTFgf2ObE2ZaDtNijXbuLmzaorZCYkq0dMWnkYp
cP5LrcECgYEA4+Sk/uND5dDc1TXRKGaiu9t2DscUpQCwk5PVOMMA0iwBJBodt85o
nDpxxecx1Mh1+0V46bnrYrIrAGurqCbQN8B5EsB9dcLZ3bg86KRwP3ZbQNxKPIqo
T16cNmHSk5jIiR6odFmV6KPjvXhjFTUYxOIjFIWNItOhXBrBxG3NyVE=
-----END RSA PRIVATE KEY-----`

	certVarName := "RP_TEST_CERT"
	keyVarName := "RP_TEST_KEY"
	truststoreVarName := "RP_TEST_TRUSTSTORE"

	tests := []struct {
		name           string
		keyFile        string
		certFile       string
		truststoreFile string
		defaultVal     func() (*config.TLS, error)
		before         func(*testing.T, afero.Fs)
		cleanup        func()
		expectedErrMsg string
	}{{
		name: "it should return the default value provided if none are set",
		defaultVal: func() (*config.TLS, error) {
			return &config.TLS{
				CertFile:       "default-cert.pem",
				KeyFile:        "default-key.pem",
				TruststoreFile: "default-trust.pem",
			}, nil
		},
		before: func(t *testing.T, fs afero.Fs) {
			require.NoError(t, afero.WriteFile(fs, "default-cert.pem", []byte(certContents), 0755))
			require.NoError(t, afero.WriteFile(fs, "default-key.pem", []byte(keyContents), 0755))
			require.NoError(t, afero.WriteFile(fs, "default-trust.pem", []byte(truststoreContents), 0755))
		},
	}, {
		name:           "it should fail if certFile is present but keyFile is empty",
		certFile:       "cert.pem",
		truststoreFile: "trust.pem",
		expectedErrMsg: "if a TLS client certificate is set, then its key must be passed to enable TLS authentication",
	}, {
		name:           "it should fail if keyFile is present but certFile is empty",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		expectedErrMsg: "if a TLS client certificate key is set, then its certificate must be passed to enable TLS authentication",
	}, {
		name:           "it should build the config with only a truststore",
		truststoreFile: "trust.pem",
		before: func(t *testing.T, fs afero.Fs) {
			require.NoError(t, afero.WriteFile(fs, "trust.pem", []byte(truststoreContents), 0755))
		},
	}, {
		name:           "it should build the config with all fields",
		certFile:       "cert.pem",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		before: func(t *testing.T, fs afero.Fs) {
			require.NoError(t, afero.WriteFile(fs, "cert.pem", []byte(certContents), 0755))
			require.NoError(t, afero.WriteFile(fs, "key.pem", []byte(keyContents), 0755))
			require.NoError(t, afero.WriteFile(fs, "trust.pem", []byte(truststoreContents), 0755))
		},
	}, {
		name: "it should pick up the values from env vars if the vars' values is empty",
		before: func(t *testing.T, fs afero.Fs) {
			cert, key, truststore := "./node.crt", "./node.key", "./ca.crt"
			os.Setenv(certVarName, cert)
			os.Setenv(keyVarName, key)
			os.Setenv(truststoreVarName, truststore)

			require.NoError(t, afero.WriteFile(fs, cert, []byte(certContents), 0755))
			require.NoError(t, afero.WriteFile(fs, key, []byte(keyContents), 0755))
			require.NoError(t, afero.WriteFile(fs, truststore, []byte(truststoreContents), 0755))
		},
		cleanup: func() {
			os.Unsetenv(certVarName)
			os.Unsetenv(keyVarName)
			os.Unsetenv(truststoreVarName)
		},
	}, {
		name:           "it should give priority to values set through the flags",
		certFile:       "cert.pem",
		keyFile:        "key.pem",
		truststoreFile: "trust.pem",
		before: func(t *testing.T, fs afero.Fs) {
			require.NoError(t, afero.WriteFile(fs, "cert.pem", []byte(certContents), 0755))
			require.NoError(t, afero.WriteFile(fs, "key.pem", []byte(keyContents), 0755))
			require.NoError(t, afero.WriteFile(fs, "trust.pem", []byte(truststoreContents), 0755))
		},
	}, {
		name: "it should return the given default value if no values are set",
		defaultVal: func() (*config.TLS, error) {
			return &config.TLS{
				CertFile:       "certificate.pem",
				KeyFile:        "cert.key",
				TruststoreFile: "ca.pem",
			}, nil
		},
		before: func(t *testing.T, fs afero.Fs) {
			require.NoError(t, afero.WriteFile(fs, "certificate.pem", []byte(certContents), 0755))
			require.NoError(t, afero.WriteFile(fs, "cert.key", []byte(keyContents), 0755))
			require.NoError(t, afero.WriteFile(fs, "ca.pem", []byte(truststoreContents), 0755))
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()
			defaultVal := tt.defaultVal
			if tt.defaultVal == nil {
				defaultVal = func() (*config.TLS, error) {
					return nil, nil
				}
			}

			if tt.before != nil {
				tt.before(st, fs)
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}

			enableTLS := false

			_, err := buildTLS(
				fs,
				&enableTLS,
				&tt.certFile,
				&tt.keyFile,
				&tt.truststoreFile,
				certVarName,
				keyVarName,
				truststoreVarName,
				defaultVal,
			)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
		})
	}
}

func TestCreateAdmin(t *testing.T) {
	tests := []struct {
		name           string
		brokers        func() []string
		configuration  func() (*config.Config, error)
		tlsConfig      func() (*tls.Config, error)
		authConfig     func() (*config.SASL, error)
		expectedErrMsg string
	}{{
		name: "the returned closure should fail if configuration fails",
		configuration: func() (*config.Config, error) {
			return nil, errors.New("No config found here")
		},
		expectedErrMsg: "No config found here",
	}, {
		name: "the returned closure should fail if tlsConfig fails",
		tlsConfig: func() (*tls.Config, error) {
			return nil, errors.New("bad tls conf")
		},
		expectedErrMsg: "bad tls conf",
	}, {
		name: "the returned closure should fail if authConfig returns an error other than ErrNoCredentials",
		authConfig: func() (*config.SASL, error) {
			return nil, errors.New("Some bad error")
		},
		expectedErrMsg: "Some bad error",
	}, {
		name: "the returned closure shouldn't fail due to authConfig returning ErrNoCredentials",
		authConfig: func() (*config.SASL, error) {
			return nil, ErrNoCredentials
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			brokers := func() []string {
				return []string{}
			}
			if tt.brokers != nil {
				brokers = tt.brokers
			}

			configuration := func() (*config.Config, error) {
				return config.Default(), nil
			}
			if tt.configuration != nil {
				configuration = tt.configuration
			}

			tlsConfig := func() (*tls.Config, error) {
				return nil, nil
			}
			if tt.tlsConfig != nil {
				tlsConfig = tt.tlsConfig
			}

			authConfig := func() (*config.SASL, error) {
				return nil, nil
			}
			if tt.authConfig != nil {
				authConfig = tt.authConfig
			}

			fn := CreateAdmin(brokers, configuration, tlsConfig, authConfig)
			_, err := fn()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			// The admin always fails to initialize because the brokers closure
			// always returns an empty slice.
			require.EqualError(st, err, "couldn't connect to redpanda at . Try using --brokers to specify other brokers to connect to.")
		})
	}
}
func TestCreateClient(t *testing.T) {
	tests := []struct {
		name           string
		brokers        func() []string
		configuration  func() (*config.Config, error)
		tlsConfig      func() (*tls.Config, error)
		authConfig     func() (*config.SASL, error)
		expectedErrMsg string
	}{{
		name: "the returned closure should fail if configuration fails",
		configuration: func() (*config.Config, error) {
			return nil, errors.New("No config found here")
		},
		expectedErrMsg: "No config found here",
	}, {
		name: "the returned closure should fail if tlsConfig fails",
		tlsConfig: func() (*tls.Config, error) {
			return nil, errors.New("bad tls conf")
		},
		expectedErrMsg: "bad tls conf",
	}, {
		name: "the returned closure should fail if authConfig returns an error other than ErrNoCredentials",
		authConfig: func() (*config.SASL, error) {
			return nil, errors.New("Some bad error")
		},
		expectedErrMsg: "Some bad error",
	}, {
		name: "the returned closure shouldn't fail due to authConfig returning ErrNoCredentials",
		authConfig: func() (*config.SASL, error) {
			return nil, ErrNoCredentials
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			brokers := func() []string {
				return []string{}
			}
			if tt.brokers != nil {
				brokers = tt.brokers
			}

			configuration := func() (*config.Config, error) {
				return config.Default(), nil
			}
			if tt.configuration != nil {
				configuration = tt.configuration
			}

			tlsConfig := func() (*tls.Config, error) {
				return nil, nil
			}
			if tt.tlsConfig != nil {
				tlsConfig = tt.tlsConfig
			}

			authConfig := func() (*config.SASL, error) {
				return nil, nil
			}
			if tt.authConfig != nil {
				authConfig = tt.authConfig
			}

			fn := CreateClient(brokers, configuration, tlsConfig, authConfig)
			_, err := fn()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			// The client always fails to initialize because the brokers closure
			// always returns an empty slice.
			require.EqualError(st, err, "couldn't connect to redpanda at . Try using --brokers to specify other brokers to connect to.")
		})
	}
}
