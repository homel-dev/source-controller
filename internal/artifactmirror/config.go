/*
Copyright 2026 The Flux authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package artifactmirror

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/labels"
)

const (
	defaultRecheckInterval  = time.Hour
	defaultOperationTimeout = 5 * time.Minute
)

type Config struct {
	Enabled          bool   `json:"enabled"`
	Endpoint         string `json:"endpoint"`
	Bucket           string `json:"bucket"`
	Prefix           string `json:"prefix"`
	ClusterID        string `json:"clusterID"`
	Region           string `json:"region"`
	Insecure         bool   `json:"insecure"`
	Selector         string `json:"selector"`
	RecheckInterval  string `json:"recheckInterval"`
	OperationTimeout string `json:"operationTimeout"`
	AccessKey        string `json:"accessKey"`
	SecretKey        string `json:"secretKey"`

	RecheckIntervalDuration  time.Duration `json:"-"`
	OperationTimeoutDuration time.Duration `json:"-"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is required")
	}
	if !c.Enabled {
		return nil
	}

	if c.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if c.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if c.ClusterID == "" {
		return fmt.Errorf("clusterID is required")
	}
	if c.Selector == "" {
		return fmt.Errorf("selector is required when mirror is enabled")
	}
	if _, err := labels.Parse(c.Selector); err != nil {
		return fmt.Errorf("invalid selector: %w", err)
	}
	if c.AccessKey == "" || c.SecretKey == "" {
		return fmt.Errorf("accessKey and secretKey are required when mirror is enabled")
	}

	if c.RecheckInterval != "" {
		d, err := time.ParseDuration(c.RecheckInterval)
		if err != nil {
			return fmt.Errorf("invalid recheckInterval: %w", err)
		}
		c.RecheckIntervalDuration = d
	} else if c.RecheckIntervalDuration == 0 {
		c.RecheckIntervalDuration = defaultRecheckInterval
	}
	if c.RecheckIntervalDuration <= 0 {
		return fmt.Errorf("recheckInterval must be greater than zero")
	}

	if c.OperationTimeout != "" {
		d, err := time.ParseDuration(c.OperationTimeout)
		if err != nil {
			return fmt.Errorf("invalid operationTimeout: %w", err)
		}
		c.OperationTimeoutDuration = d
	} else if c.OperationTimeoutDuration == 0 {
		c.OperationTimeoutDuration = defaultOperationTimeout
	}
	if c.OperationTimeoutDuration <= 0 {
		return fmt.Errorf("operationTimeout must be greater than zero")
	}

	return nil
}
