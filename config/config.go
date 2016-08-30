// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period time.Duration `config:"period"`
	Servers []Server `config:"servers"`
}

type Server struct {
	Code string `config:"code"`
	Connection struct {
		URL string `config:"url"`
		Username string `config:"username"`
		Password string `config:"password"`
		UserAgent string `config:"user_agent"`
		UserAgentPassword string `config:"user_agent_password"`
		RetsVersion string `config:"rets_version"`
	} `config:"connection"`
	Status []string `config:"status"`
	Type []string `config:"type"`
	Custom []Custom `config:"custom"`
}

type Custom struct {
	Resource string `config:"resource"`
	Class string `config:"class"`
	Key string `config:"key"`
	Query string `config:"query"`
}

var DefaultConfig = Config{
	Period: 1 * time.Second,
}
