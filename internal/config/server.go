package config

type ServerConfig struct {
	Port           string `yaml:"port"`
	AllowedOrigins string `yaml:"allowed_origins"`
	DebugAPILogs   bool   `yaml:"debug_api_logs"`
}

type OTLPConfig struct {
	GRPCPort             string `yaml:"grpc_port"`
	HTTPPort             string `yaml:"http_port"`
	GRPCMaxConcurrentStr uint32 `yaml:"grpc_max_concurrent_streams"`
	GRPCMaxRecvMsgSize   int    `yaml:"grpc_max_recv_msg_size"`
}
