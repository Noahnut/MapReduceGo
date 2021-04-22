module github.com/Noahnut/mapReduce

go 1.16

require (
	google.golang.org/grpc v1.37.0
	google.golang.org/protobuf v1.26.0-rc.1
)

replace google.golang.org/grpc => github.com/grpc/grpc-go v1.37.0

replace google.golang.org/protobuf => github.com/protocolbuffers/protobuf-go v1.26.0
