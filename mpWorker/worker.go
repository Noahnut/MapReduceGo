package mpWoker

import (
	"context"
	"log"
	"sync"

	"github.com/Noahnut/mapReduce/mpProtoc"
	"google.golang.org/grpc"
)

type _workerState int
type _taskType int

const (
	_map    _taskType = iota
	_reduce _taskType = iota
	_none   _taskType = iota
)

const (
	_idle     _workerState = iota
	_progress _workerState = iota
	_complete _workerState = iota
	_die      _workerState = iota
)

type MpWorker struct {
	_mpWorkerClient mpProtoc.MapReduceServiceClient
	_mpData         []byte
	_workerIP       string
	_workState      _workerState
}

func (this *MpWorker) keepAlive(ctx context.Context) {
	log.Println("Start Send keepAlive")
	for {
		select {
		case <-ctx.Done():
			log.Println("keepAlive exit")
		default:
			_, err := this._mpWorkerClient.KeepAlive(context.Background(), &mpProtoc.WorkerState{IPAddress: "127.0.0.1", WorkerState: int32(_idle)})
			log.Println(err)
		}
	}
}

func (this *MpWorker) wokerMain(ctx context.Context) {
	go this.keepAlive(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Println("worker Main exit")
		default:

		}
	}
}

func (this *MpWorker) CreateMpWorker(masterIP string, workerIP string, port string) {
	var wg sync.WaitGroup

	conn, err := grpc.Dial(":"+port, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Can not create grpc error: %v", err)
	}
	defer conn.Close()
	this._mpWorkerClient = mpProtoc.NewMapReduceServiceClient(conn)
	this._workerIP = workerIP
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	wg.Add(1)
	go this.wokerMain(ctx)
	wg.Wait()
}
