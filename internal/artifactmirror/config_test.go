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
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/gomega"
)

func TestLoadConfig(t *testing.T) {
	g := NewWithT(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "mirror.json")
	data := []byte(`{
		"enabled": true,
		"endpoint": "minio.example:9000",
		"bucket": "artifacts",
		"prefix": "snapshots",
		"clusterID": "cluster-1",
		"selector": "memory.homel.dev/index=true",
		"recheckInterval": "15m",
		"operationTimeout": "45s",
		"accessKey": "access",
		"secretKey": "secret"
	}`)
	g.Expect(os.WriteFile(path, data, 0o600)).To(Succeed())

	cfg, err := LoadConfig(path)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(cfg.RecheckIntervalDuration).To(Equal(15 * time.Minute))
	g.Expect(cfg.OperationTimeoutDuration).To(Equal(45 * time.Second))
}

func TestConfigValidateRejectsUnsafeEnabledConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing endpoint", func(c *Config) { c.Endpoint = "" }},
		{"missing bucket", func(c *Config) { c.Bucket = "" }},
		{"missing cluster ID", func(c *Config) { c.ClusterID = "" }},
		{"missing selector", func(c *Config) { c.Selector = "" }},
		{"invalid selector", func(c *Config) { c.Selector = "not a selector==" }},
		{"missing access key", func(c *Config) { c.AccessKey = "" }},
		{"missing secret key", func(c *Config) { c.SecretKey = "" }},
		{"zero recheck interval", func(c *Config) {
			c.RecheckInterval = "0s"
		}},
		{"zero operation timeout", func(c *Config) {
			c.OperationTimeout = "0s"
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			cfg := validConfig()
			tt.mutate(cfg)
			g.Expect(cfg.Validate()).To(HaveOccurred())
		})
	}
}

func validConfig() *Config {
	return &Config{
		Enabled:          true,
		Endpoint:         "minio.example:9000",
		Bucket:           "artifacts",
		Prefix:           "snapshots",
		ClusterID:        "cluster-1",
		Selector:         "mirror=true",
		RecheckInterval:  "15m",
		OperationTimeout: "45s",
		AccessKey:        "access",
		SecretKey:        "secret",
	}
}
