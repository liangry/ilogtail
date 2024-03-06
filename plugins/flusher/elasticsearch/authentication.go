// Copyright 2023 iLogtail Authors
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

package elasticsearch

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/alibaba/ilogtail/pkg/tlscommon"
)

type Authentication struct {
	// PlainTextConfig
	PlainText *PlainTextConfig
	// TLS authentication
	TLS *tlscommon.TLSConfig
}

type PlainTextConfig struct {
	// The username for connecting to clickhouse.
	Username string
	// The password for connecting to clickhouse.
	Password string
}

func (config *Authentication) ConfigureAuthenticationAndHTTP(httpcfg *HTTPConfig, opts *elasticsearch.Config) error {
	if config.PlainText != nil {
		if err := config.PlainText.ConfigurePlaintext(opts); err != nil {
			return err
		}
	}

	transport := &http.Transport{}
	if config.TLS != nil {
		tlsConfig, err := config.TLS.LoadTLSConfig()
		if err != nil {
			return fmt.Errorf("error loading tls config: %w", err)
		}
		if tlsConfig != nil {
			transport.TLSClientConfig = tlsConfig
		}
		if config.TLS.CAFile != "" {
			opts.CACert, err = os.ReadFile(config.TLS.CAFile)
			if err != nil {
				return err
			}
		}
	}
	if httpcfg != nil {
		if httpcfg.MaxIdleConns != 0 {
			transport.MaxIdleConns = httpcfg.MaxIdleConns
		} else {
			transport.MaxIdleConns = 100
		}

		if httpcfg.MaxIdleConnsPerHost != 0 {
			transport.MaxIdleConnsPerHost = httpcfg.MaxIdleConnsPerHost
		} else {
			transport.MaxIdleConnsPerHost = 4
		}

		if httpcfg.MaxConnsPerHost != 0 {
			transport.MaxConnsPerHost = httpcfg.MaxConnsPerHost
		} else {
			transport.MaxConnsPerHost = 4
		}

		if httpcfg.IdleConnTimeout != 0 {
			transport.IdleConnTimeout = time.Duration(httpcfg.IdleConnTimeout) * time.Second
		} else {
			transport.IdleConnTimeout = 90 * time.Second
		}

		if httpcfg.ResponseHeaderTimeout != 0 {
			transport.ResponseHeaderTimeout = time.Duration(httpcfg.ResponseHeaderTimeout) * time.Second
		} else {
			transport.ResponseHeaderTimeout = 90 * time.Second
		}

		if httpcfg.WriteBufferKiloBytes != 0 {
			transport.WriteBufferSize = httpcfg.WriteBufferKiloBytes << 10
		} else {
			transport.WriteBufferSize = 4096
		}
	}
	opts.Transport = transport

	return nil
}

func (plainTextConfig *PlainTextConfig) ConfigurePlaintext(opts *elasticsearch.Config) error {
	// Validate Auth info
	if plainTextConfig.Username == "" || plainTextConfig.Password == "" {
		return fmt.Errorf("PlainTextConfig username or password cannot be null")
	}
	opts.Username = plainTextConfig.Username
	opts.Password = plainTextConfig.Password
	return nil
}
