/*
 * Copyright (c) 2024 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"io/ioutil"
	"os"
	"reflect"
	"regexp"

	"strings"

	logging "github.com/op/go-logging"
	yaml "gopkg.in/yaml.v2"
)

var log = logging.MustGetLogger("profile")
var Cfg *ProfileConfig

type Config struct {
	ProfileConfig ProfileConfig `yaml:"profile"`
}

type ProfileConfig struct {
	LogFile         string       `default:"/var/log/profile.log" yaml:"log-file"`
	LogLevel        string       `default:"info" yaml:"log-level"`
	ListenPort      int          `default:"20419" yaml:"listen-port"`
	FlameQueryLimit int          `default:"1000000" yaml:"flame_query_limit"`
	Querier         Querier      `yaml:"querier"`
	ResultFilter    ResultFilter `yaml:"result-filter"`
}

type Querier struct {
	Host string `default:"127.0.0.1" yaml:"host"`
	Port int    `default:"20416" yaml:"port"`
}

type ResultFilter struct {
	Disabled          bool    `default:"false" yaml:"disabled"`
	TotalValuePercent float64 `default:"0.1" yaml:"total-value-percent"`
	Depth             int     `default:"12" yaml:"depth"`
}

func (c *Config) expendEnv() {
	reConfig := reflect.ValueOf(&c.ProfileConfig)
	reConfig = reConfig.Elem()
	for i := 0; i < reConfig.NumField(); i++ {
		field := reConfig.Field(i)
		switch field.Type().String() {
		case "string":
			fieldStr := field.String()
			pattern := regexp.MustCompile(`\$\{(.+?)\}`)
			p := pattern.FindAllSubmatch([]byte(fieldStr), -1)
			for _, i := range p {
				str := string(i[1])
				fieldStr = strings.Replace(fieldStr, string(i[0]), os.Getenv(str), 1)
			}
			field.SetString(fieldStr)
		}
	}
}

func (c *Config) Validate() error {
	return nil
}

func (c *Config) Load(path string) {
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		log.Error("Read config file error:", err, path)
		os.Exit(1)
	}

	if err = yaml.Unmarshal(configBytes, c); err != nil {
		log.Error("Unmarshal yaml error:", err)
		os.Exit(1)
	}

	if err = c.Validate(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
	c.expendEnv()
}

func DefaultConfig() *Config {
	cfg := &Config{}
	if err := SetDefault(cfg); err != nil {
		log.Error(err)
		os.Exit(1)
	}
	return cfg
}
