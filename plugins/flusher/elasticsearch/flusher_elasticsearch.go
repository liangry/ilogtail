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
	"errors"
	"fmt"
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
	// Flush interval in milliseconds
	FlushIntervalMs int
	// Max flush retries if error returned
	MaxFlushRetries int
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
		FlushIntervalMs: 2000,
		MaxFlushRetries: 10,
		WorkerNumber:    4,
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
	<-f.workerPool
	go func() {
		defer func() {
			f.workerPool <- struct{}{}
		}()
		f.flushLogGroupList(logGroupList)
	}()
	return nil
}

func (f *FlusherElasticSearch) flushLogGroupList(logGroupList []*protocol.LogGroup) {
	for _, logGroup := range logGroupList {
		f.flushLogGroup(logGroup)
	}
}

func (f *FlusherElasticSearch) flushLogGroup(logGroup *protocol.LogGroup) {
	serializedLogs, values, err := f.converter.ToByteStreamWithSelectedFields(logGroup, f.indexKeys)
	if err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch convert log fail, error", err)
		return
	}

	var builder strings.Builder
	nowTime := time.Now().Local()
	for index, log := range serializedLogs.([][]byte) {
		ESIndex := &f.Index
		if f.isDynamicIndex {
			valueMap := values[index]
			ESIndex, err = fmtstr.FormatIndex(valueMap, f.Index, uint32(nowTime.Unix()))
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
	routing := ""
	for _, tag := range logGroup.LogTags {
		if tag.Key == "__pack_id__" {
			routing = tag.Value
			break
		}
	}

	req := esapi.BulkRequest{
		Body:    strings.NewReader(builder.String()),
		Routing: routing,
	}

	var res *esapi.Response
	var latency time.Duration
	for attempt := 0; attempt <= f.MaxFlushRetries; attempt++ {
		start := time.Now()
		res, err = req.Do(context.Background(), f.esClient)
		latency = time.Since(start)
		if err != nil || res.StatusCode == 429 {
			logger.Warning(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch error or too many requests, attempt", attempt+1)
			if attempt < f.MaxFlushRetries {
				time.Sleep(time.Millisecond * time.Duration(f.FlushIntervalMs*(attempt+1)))
				req = esapi.BulkRequest{
					Body:    strings.NewReader(builder.String()),
					Routing: routing,
				}
				continue
			}
		}
		break
	}

	if err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch request fail after retries, error", err)
		return
	}

	defer res.Body.Close()
	if res.StatusCode >= 400 && res.StatusCode <= 499 {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch request client error", res)
	} else if res.StatusCode >= 500 && res.StatusCode <= 599 {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch request server error", res)
	} else {
		logger.Info(f.context.GetRuntimeContext(), "log tags", logGroup.LogTags, "serialized logcount", len(serializedLogs.([][]byte)), "routing", routing, "latency", latency.Milliseconds())
	}
}

func init() {
	pipeline.Flushers["flusher_elasticsearch"] = func() pipeline.Flusher {
		f := NewFlusherElasticSearch()
		return f
	}
}
