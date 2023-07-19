// Copyright 2023 iLogtail Authors
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

package model

import proto "github.com/alibaba/ilogtail/config_server/service/proto"

type AgentGroupTag struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

func (t *AgentGroupTag) ToProto() *proto.AgentGroupTag {
	pt := new(proto.AgentGroupTag)
	pt.Name = t.Name
	pt.Value = t.Value
	return pt
}

func (t *AgentGroupTag) ParseProto(pt *proto.AgentGroupTag) {
	t.Name = pt.Name
	t.Value = pt.Value
}
