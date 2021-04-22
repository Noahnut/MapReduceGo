package mpMaster

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Noahnut/mapReduce/mpProtoc"
	"google.golang.org/grpc"
)

type _workerState int
type _taskType int
type _ipAddress string

const KB = 1024
const MB = KB * 1024

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

type worker struct {
	state         _workerState
	task          _taskType
	lastAliveTime time.Time
	data          []byte
}

type MpMaster struct {
	masterServer _masterServer
}

type _workers struct {
	workers map[_ipAddress]worker
}

type _masterServer struct {
	mpProtoc.UnimplementedMapReduceServiceServer
	rwLock          sync.RWMutex
	workers         _workers
	dataPath        []string
	reduceDataURL   []string
	currentData     []byte
	splitDataNumber int
	splitSize       int
}

func (this *_masterServer) readDataFromPath() error {
	path := this.dataPath[0]
	this.dataPath = this.dataPath[1:len(this.dataPath)]
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	dataSize := len(data)
	fmt.Println(dataSize)
	if dataSize >= (MB) {
		this.splitSize = 64 * KB
	} else if dataSize >= (KB) {
		this.splitSize = 64
	} else {
		this.splitSize = 32
	}
	this.splitDataNumber = dataSize / this.splitSize
	this.currentData = data
	return nil
}

func (this *_masterServer) readDataRoutine(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("readDataFromPath Done")
			return
		default:
			if len(this.dataPath) > 0 && len(this.currentData) == 0 {
				err := this.readDataFromPath()
				log.Println(err.Error())
			} else {
				time.Sleep(time.Second)
			}
		}
	}
}

func (this *_masterServer) checkWorkerAliveTimeOut() {
	for k, v := range this.workers.workers {
		currentTime := time.Now()
		if currentTime.Sub(v.lastAliveTime).Seconds() > float64(time.Second*60) {
			log.Printf("%s Worker Time out\n", k)
			v.state = _die
			if v.task == _map {
				this.rwLock.Lock()
				this.currentData = append(this.currentData, v.data...)
				this.splitDataNumber++
				this.rwLock.Unlock()
			}
			v.data = nil
		}
	}
}

func (this *_masterServer) checkWorkerHealth(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("checkWorkerHealth Done")
		default:
			this.checkWorkerAliveTimeOut()
		}
	}
}

func (this *_masterServer) giveMapTaskToWorker(worker *worker) {
	this.rwLock.Lock()
	worker.data = this.currentData[0:this.splitSize]
	this.currentData = this.currentData[this.splitSize:len(this.currentData)]
	this.splitDataNumber--
	this.rwLock.Unlock()
	worker.state = _progress
	worker.task = _map
}

/*
receive the worker keep alive
if the IP is not in the worker table add to the new worker
*/
func (this *_masterServer) KeepAlive(ctx context.Context, in *mpProtoc.WorkerState) (*mpProtoc.MasterRequest, error) {
	workIP := in.IPAddress
	log.Printf("rec %s worker keep alive", workIP)
	workReplyState := in.WorkerState
	if _, ok := this.workers.workers[_ipAddress(workIP)]; !ok {
		newWork := worker{
			state: _workerState(workReplyState),
			task:  _none,
		}

		this.workers.workers[_ipAddress(workIP)] = newWork
	}

	worker := this.workers.workers[_ipAddress(workIP)]
	worker.lastAliveTime = time.Now()

	if worker.state == _die {
		worker.state = _idle
		workReplyState = int32(_idle)
	}

	if _workerState(workReplyState) == _complete {
		worker.state = _idle
		// The map or reduce work done
		// if the map should return the data position for reduce download
	}

	if _workerState(workReplyState) == _idle {
		if this.splitDataNumber > 0 {
			this.giveMapTaskToWorker(&worker)
		} else {
			//reduce work part
			worker.task = _reduce
			worker.state = _progress
		}
	}

	return &mpProtoc.MasterRequest{WorkerTask: int32(worker.task)}, nil
}

func (this *_masterServer) sendWorkDataToWorker(ctx context.Context, in *mpProtoc.WorkerNeed) (*mpProtoc.NeedProcessData, error) {
	workerNeed := in.Task
	MasterResponseData := mpProtoc.NeedProcessData{}
	worker := this.workers.workers[_ipAddress(in.IPAddress)]

	if workerNeed == int32(_map) {
		MasterResponseData.Data = worker.data
	} else {
		if len(this.reduceDataURL) > 0 {
			this.rwLock.Lock()
			MasterResponseData.LocalDataPosition = this.reduceDataURL[0]
			this.reduceDataURL = this.reduceDataURL[1:len(this.reduceDataURL)]
			this.rwLock.Unlock()
		}
	}

	return &MasterResponseData, nil
}

func (this *MpMaster) RunMaster(port string, dataPath []string) error {
	this.masterServer = _masterServer{dataPath: dataPath}

	this.masterServer.workers.workers = make(map[_ipAddress]worker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go this.masterServer.readDataRoutine(ctx)
	go this.masterServer.checkWorkerHealth(ctx)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("Server listening on", port)

	gRPCServer := grpc.NewServer()
	mpProtoc.RegisterMapReduceServiceServer(gRPCServer, &this.masterServer)
	if err := gRPCServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return nil
}
