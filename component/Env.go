package component

type Env struct {
}

var GlobalEnv *Env

func LoadEnvFromYaml(filepath string) error {
	var env Env
	GlobalEnv = &env
	return nil
}

func LoadEnvFromDefaultYaml() error {
	return LoadEnvFromYaml("default.yaml")
}
