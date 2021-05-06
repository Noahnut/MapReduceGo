package mpClient

import (
	"context"
	"log"

	"github.com/Noahnut/mapReduce/mpProtoc"
	"google.golang.org/grpc"
)

type MpClient struct {
	port string
}

func (client *MpClient) sendClientInfoToMaster(conn grpc.ClientConnInterface, dataPath []string, mapReduceAppPath string) {
	grpcClient := mpProtoc.NewMapReduceServiceClient(conn)
	reply, err := grpcClient.RecvMapReduceInfoToMaster(context.Background(), &mpProtoc.MapReduceInfo{DataPath: dataPath, MapReduceAppPath: mapReduceAppPath})
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Println(reply)
}

func (client *MpClient) StartMapReduce(port string, dataPath []string, mapReduceAppPath string) {
	client.port = port
	conn, err := grpc.Dial(":"+port, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Can not create grpc error: %v", err)
	}
	defer conn.Close()
	client.sendClientInfoToMaster(conn, dataPath, mapReduceAppPath)
}

func (client *MpClient) SendDataPath(dataPath []string) {
	conn, err := grpc.Dial(":"+client.port, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Can not create grpc error: %v", err)
	}
	defer conn.Close()
	client.sendClientInfoToMaster(conn, dataPath, "")
}
