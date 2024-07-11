// Copyright 2024 iLogtail Authors
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

package reserve

import (
	"fmt"
	"strings"

	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
)

type ProcessorReserve struct {
	ReserveKeys []string

	keyDictionary map[string]bool
	context       pipeline.Context
}

const pluginName = "processor_reserve"

// Init called for init some system resources, like socket, mutex...
func (p *ProcessorReserve) Init(context pipeline.Context) error {
	if len(p.ReserveKeys) == 0 {
		return fmt.Errorf("must specify ReserveKeys for plugin %v", pluginName)
	}

	p.context = context
	p.keyDictionary = make(map[string]bool)
	for _, reserveKey := range p.ReserveKeys {
		p.keyDictionary[reserveKey] = true
	}
	return nil
}

func (*ProcessorReserve) Description() string {
	return "reserve processor for logtail"
}

func (p *ProcessorReserve) ProcessLogs(logArray []*protocol.Log) []*protocol.Log {
	for _, log := range logArray {
		p.processLog(log)
	}
	return logArray
}

func (p *ProcessorReserve) processLog(log *protocol.Log) {
	result := make([]*protocol.Log_Content, 0, len(log.Contents))
	for _, content := range log.Contents {
		if _, exists := p.keyDictionary[content.Key]; exists {
			result = append(result, content)
		} else if strings.HasPrefix(content.Key, "__tag__") {
			result = append(result, content)
		}
	}
	log.Contents = result
}

func init() {
	pipeline.Processors[pluginName] = func() pipeline.Processor {
		return &ProcessorReserve{}
	}
}
