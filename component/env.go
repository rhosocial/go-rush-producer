package component

import (
	"log"
	"os"
	"strconv"

	"github.com/rhosocial/go-rush-common/component/mysql"
	models "github.com/rhosocial/go-rush-producer/models/node_info"
	"gopkg.in/yaml.v3"
)

type EnvNet struct {
	ListenPort *uint16 `yaml:"ListenPort,omitempty" default:"8080"`
}

func (e *EnvNet) GetListenPortDefault() *uint16 {
	port := uint16(8080)
	return &port
}

func (e *EnvNet) Validate() error {
	if e.ListenPort == nil {
		e.ListenPort = e.GetListenPortDefault()
	}
	return nil
}

type Env struct {
	Net          *EnvNet                 `yaml:"Net,omitempty"`
	MySQLServers *[]mysql.EnvMySQLServer `yaml:"MySQLServers,omitempty"`
	Identity     int                     `yaml:"Identity,omitempty" default:"0"`
	Master       *models.FreshNodeInfo   `yaml:"Master,omitempty"`
}

// GetNetDefault 取得 EnvNet 的默认值。
// EnvNet.ListenPort 默认值为 8080。
func (e *Env) GetNetDefault() *EnvNet {
	net := EnvNet{}
	net.ListenPort = net.GetListenPortDefault()
	return &net
}

var GlobalEnv *Env

// LoadEnvDefault 加载配置参数默认值。
func LoadEnvDefault() error {
	if GlobalEnv == nil {
		var env Env
		err := env.Validate()
		if err != nil {
			return err
		}
		GlobalEnv = &env
	}
	return nil
}

// Validate 验证并加载默认值。
// Env 的默认值包括：
// EnvNet
func (e *Env) Validate() error {
	if e.Net == nil {
		e.Net = e.GetNetDefault()
	} else if err := e.Net.Validate(); err != nil {
		return err
	}
	return nil
}

func LoadEnvFromYaml(filepath string) error {
	file, err := os.ReadFile(filepath)
	if err != nil {
		return err
	}
	if GlobalEnv == nil {
		var env Env
		err := env.Validate()
		if err != nil {
			return err
		}
		GlobalEnv = &env
	}
	if err := yaml.Unmarshal(file, GlobalEnv); err != nil {
		return err
	}
	if err := GlobalEnv.Validate(); err != nil {
		return err
	}
	return nil
}

func LoadEnvFromSystemEnvVar() error {
	if GlobalEnv == nil {
		var env Env
		err := env.Validate()
		if err != nil {
			return err
		}
		GlobalEnv = &env
	}
	if value, exist := os.LookupEnv("Producer_Net_ListenPort"); exist {
		log.Println("Producer_Net_ListenPort: ", value)
		port, _ := strconv.ParseUint(value, 10, 16)
		*(*GlobalEnv.Net).ListenPort = uint16(port)
	}
	if value, exist := os.LookupEnv("Producer_Identity"); exist {
		log.Println("Producer_Identity: ", value)
		identity, _ := strconv.ParseInt(value, 10, 32)
		GlobalEnv.Identity = int(identity)
	}
	return nil
}
