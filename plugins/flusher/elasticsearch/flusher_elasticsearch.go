// Copyright 2021 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elasticsearch

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/alibaba/ilogtail/pkg/fmtstr"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	converter "github.com/alibaba/ilogtail/pkg/protocol/converter"
)

type FlusherElasticSearch struct {
	// Convert ilogtail data convert config
	Convert convertConfig
	// Addresses elasticsearch addresses
	Addresses []string
	// Authentication
	Authentication Authentication
	// The container of logs
	Index string
	// HTTP config
	HTTPConfig *HTTPConfig
	// Retry strategy
	Retry retryConfig
	// Worker number
	WorkerNumber int

	indexKeys      []string
	isDynamicIndex bool
	context        pipeline.Context
	converter      *converter.Converter
	esClient       *elasticsearch.Client
	workerPool     chan struct{}
}

type HTTPConfig struct {
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	IdleConnTimeout       int
	ResponseHeaderTimeout int
	WriteBufferKiloBytes  int
}

type convertConfig struct {
	// Rename one or more fields from tags.
	TagFieldsRename map[string]string
	// Rename one or more fields, The protocol field options can only be: contents, tags, time
	ProtocolFieldsRename map[string]string
	// Convert protocol, default value: custom_single
	Protocol string
	// Convert encoding, default value:json
	// The options are: 'json'
	Encoding string
}

type retryConfig struct {
	Enable        bool          // If enable retry, default is true
	MaxRetryTimes int           // Max retry times, default is 10
	InitialDelay  time.Duration // Delay time before the first retry, default is 2s
	MaxDelay      time.Duration // max delay time when retry, default is 1000s
}

func NewFlusherElasticSearch() *FlusherElasticSearch {
	return &FlusherElasticSearch{
		Addresses: []string{},
		Authentication: Authentication{
			PlainText: &PlainTextConfig{
				Username: "",
				Password: "",
			},
		},
		Index: "",
		Convert: convertConfig{
			Protocol: converter.ProtocolCustomSingle,
			Encoding: converter.EncodingJSON,
		},
		Retry: retryConfig{
			Enable:        true,
			MaxRetryTimes: 10,
			InitialDelay:  2 * time.Second,
			MaxDelay:      1000 * time.Second,
		},
		WorkerNumber: 4,
	}
}

func (f *FlusherElasticSearch) Init(context pipeline.Context) error {
	f.context = context
	// Validate config of flusher
	if err := f.Validate(); err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_INIT_ALARM", "init elasticsearch flusher fail, error", err)
		return err
	}
	// Set default value while not set
	if f.Convert.Encoding == "" {
		f.converter.Encoding = converter.EncodingJSON
	}
	if f.Convert.Protocol == "" {
		f.Convert.Protocol = converter.ProtocolCustomSingle
	}
	// Init converter
	convert, err := f.getConverter()
	if err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_INIT_ALARM", "init elasticsearch flusher converter fail, error", err)
		return err
	}
	f.converter = convert

	// Init index keys
	indexKeys, isDynamicIndex, err := f.getIndexKeys()
	if err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_INIT_ALARM", "init elasticsearch flusher index fail, error", err)
		return err
	}
	f.indexKeys = indexKeys
	f.isDynamicIndex = isDynamicIndex

	cfg := elasticsearch.Config{
		Addresses: f.Addresses,
	}
	if err = f.Authentication.ConfigureAuthenticationAndHTTP(f.HTTPConfig, &cfg); err != nil {
		err = fmt.Errorf("configure authenticationfailed, err: %w", err)
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_INIT_ALARM", "init elasticsearch flusher error", err)
		return err
	}

	f.esClient, err = elasticsearch.NewClient(cfg)
	if err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_INIT_ALARM", "create elasticsearch client error", err)
		return err
	}

	f.workerPool = make(chan struct{}, f.WorkerNumber)
	for i := 0; i < f.WorkerNumber; i++ {
		f.workerPool <- struct{}{}
	}
	return nil
}

func (f *FlusherElasticSearch) Description() string {
	return "ElasticSearch flusher for logtail"
}

func (f *FlusherElasticSearch) Validate() error {
	if f.Addresses == nil || len(f.Addresses) == 0 {
		var err = fmt.Errorf("elasticsearch addrs is nil")
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_INIT_ALARM", "init elasticsearch flusher error", err)
		return err
	}
	return nil
}

func (f *FlusherElasticSearch) getConverter() (*converter.Converter, error) {
	logger.Debug(f.context.GetRuntimeContext(), "[ilogtail data convert config] Protocol", f.Convert.Protocol,
		"Encoding", f.Convert.Encoding, "TagFieldsRename", f.Convert.TagFieldsRename, "ProtocolFieldsRename", f.Convert.ProtocolFieldsRename)
	return converter.NewConverter(f.Convert.Protocol, f.Convert.Encoding, f.Convert.TagFieldsRename, f.Convert.ProtocolFieldsRename)
}

func (f *FlusherElasticSearch) getIndexKeys() ([]string, bool, error) {
	if f.Index == "" {
		return nil, false, errors.New("index can't be empty")
	}

	// Obtain index keys from dynamic index expression
	compileKeys, err := fmtstr.CompileKeys(f.Index)
	if err != nil {
		return nil, false, err
	}
	// CompileKeys() parse all variables inside %{}
	// but indexKeys is used to find field express starting with 'content.' or 'tag.'
	// so date express starting with '+' should be ignored
	indexKeys := make([]string, 0, len(compileKeys))
	for _, key := range compileKeys {
		if key[0] != '+' {
			indexKeys = append(indexKeys, key)
		}
	}
	isDynamicIndex := len(compileKeys) > 0
	return indexKeys, isDynamicIndex, nil
}

func (f *FlusherElasticSearch) IsReady(projectName string, logstoreName string, logstoreKey int64) bool {
	return f.esClient != nil
}

func (f *FlusherElasticSearch) SetUrgent(flag bool) {}

func (f *FlusherElasticSearch) Stop() error {
	return nil
}

func (f *FlusherElasticSearch) Flush(projectName string, logstoreName string, configName string, logGroupList []*protocol.LogGroup) error {
	for _, v := range logGroupList {
		<-f.workerPool
		go func(logGroup *protocol.LogGroup) {
			defer func() {
				f.workerPool <- struct{}{}
			}()
			f.flushWithRetry(logGroup)
		}(v)
	}
	return nil
}

func (f *FlusherElasticSearch) flushWithRetry(logGroup *protocol.LogGroup) error {
	serializedLogs, values, err := f.converter.ToByteStreamWithSelectedFields(logGroup, f.indexKeys)
	if err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch convert log fail, error", err)
		return err
	}
	logs := serializedLogs.([][]byte)
	body := f.buildBody(logs, values)
	routing := f.extractRouting(logGroup.LogTags)

	attempts := 0
	var latency time.Duration
	for ; attempts <= f.Retry.MaxRetryTimes; attempts++ {
		ok, retryable, e, lat := f.flushLogGroup(body, routing)
		err = e
		latency = lat
		if ok || !retryable || !f.Retry.Enable || attempts == f.Retry.MaxRetryTimes {
			break
		}
		wait := f.getNextRetryDelay(attempts)
		logger.Warning(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "retryable error", err, "logcount", len(logs), "tags", logGroup.LogTags, "routing", routing, "attempts", attempts, "wait", wait)
		time.Sleep(wait)
	}
	if err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "non-retryable error", err, "logcount", len(logs), "tags", logGroup.LogTags, "routing", routing, "attempts", attempts)
		return err
	}

	logger.Info(f.context.GetRuntimeContext(), "bulk request success, latency", latency.Milliseconds(), "logcount", len(logs), "tags", logGroup.LogTags, "routing", routing, "attempts", attempts)
	return nil
}

func (f *FlusherElasticSearch) buildBody(serializedLogs [][]byte, values []map[string]string) string {
	var builder strings.Builder
	current := time.Now().Local()
	for index, log := range serializedLogs {
		ESIndex := &f.Index
		if f.isDynamicIndex {
			valueMap := values[index]
			var err error
			ESIndex, err = fmtstr.FormatIndex(valueMap, f.Index, uint32(current.Unix()))
			if err != nil {
				logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch format index fail, error", err)
				continue
			}
		}
		builder.WriteString(`{"index": {"_index": "`)
		builder.WriteString(*ESIndex)
		builder.WriteString(`"}}`)
		builder.WriteString("\n")
		builder.Write(log)
		builder.WriteString("\n")
	}
	return builder.String()
}

func (f *FlusherElasticSearch) extractRouting(tags []*protocol.LogTag) string {
	for _, tag := range tags {
		if tag.Key == "__pack_id__" {
			return tag.Value
		}
	}
	return ""
}

func (f *FlusherElasticSearch) flushLogGroup(body string, routing string) (bool, bool, error, time.Duration) {
	req := esapi.BulkRequest{
		Body:    strings.NewReader(body),
		Routing: routing,
	}

	start := time.Now()
	res, err := req.Do(context.Background(), f.esClient)
	latency := time.Since(start)
	if err != nil {
		return false, false, err, latency
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= http.StatusInternalServerError {
		return false, true, errors.New(res.Status()), latency
	} else if res.StatusCode >= http.StatusBadRequest {
		return false, false, errors.New(res.Status()), latency
	}

	return true, false, nil, latency
}

func (f *FlusherElasticSearch) getNextRetryDelay(retryTime int) time.Duration {
	delay := f.Retry.InitialDelay * (1 << retryTime)
	if delay > f.Retry.MaxDelay {
		delay = f.Retry.MaxDelay
	}

	// apply about equaly distributed jitter in second half of the interval, such that the wait
	// time falls into the interval [dur/2, dur]
	harf := int64(delay / 2)
	jitter, err := rand.Int(rand.Reader, big.NewInt(harf+1))
	if err != nil {
		return delay
	}
	return time.Duration(harf + jitter.Int64())
}

func init() {
	pipeline.Flushers["flusher_elasticsearch"] = func() pipeline.Flusher {
		f := NewFlusherElasticSearch()
		return f
	}
}
