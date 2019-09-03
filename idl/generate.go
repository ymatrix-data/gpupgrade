package idl

// Creates the .pb.go protobuf definitions.
//go:generate protoc --go_out=plugins=grpc:. cli_to_hub.proto hub_to_agent.proto

// Generates mocks for the above definitions.
//go:generate mockgen -source cli_to_hub.pb.go   -destination mock_idl/mock_cli_to_hub.pb.go
//go:generate mockgen -source hub_to_agent.pb.go -destination mock_idl/mock_hub_to_agent.pb.go
