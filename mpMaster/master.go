package mpMaster

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"sync"
	"time"

	"github.com/Noahnut/mapReduce/mpProtoc"
	"google.golang.org/grpc"
)

// Process to slow becasue the per chunk data size is too small many cost on the network communication

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
	ipAddress     string
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
	rwLock           sync.RWMutex
	workers          _workers
	dataPath         []string
	reduceDataURL    map[string]int
	currentData      []byte
	mapReduceAppPath string
	splitDataNumber  int
	splitSize        int
}

func (master *_masterServer) countSplitDataNumberAndSize(dataSize int) {
	log.Printf("data size %d", dataSize)
	if dataSize >= (MB) {
		master.splitSize = 64 * KB
	} else if dataSize >= (KB) {
		master.splitSize = KB
	} else {
		master.splitSize = 64
	}
	master.splitDataNumber = dataSize / master.splitSize
	log.Printf("splitDataNumber %d", master.splitDataNumber)
}

func (master *_masterServer) readDataFromPath() error {
	path := master.dataPath[0]
	master.rwLock.Lock()
	master.dataPath = master.dataPath[1:len(master.dataPath)]
	master.rwLock.Unlock()
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	master.countSplitDataNumberAndSize(len(data))
	master.currentData = data
	return nil
}

func (master *_masterServer) readDataRoutine(ctx context.Context) {
	keepAliveTick := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Println("readDataFromPath Done")
			return
		case <-keepAliveTick.C:
			if len(master.dataPath) > 0 && len(master.currentData) == 0 {
				err := master.readDataFromPath()
				if err != nil {
					log.Println(err)
				}
			}
		}
	}
}

/**
Check worker lastalivetime if worker timeout set the worker state to _die and recycle the data back to the master
**/
func (master *_masterServer) checkWorkerAliveTimeOut() {
	for k, v := range master.workers.workers {
		currentTime := time.Now()
		if currentTime.Sub(v.lastAliveTime).Seconds() > float64(time.Second*60) {
			log.Printf("%s Worker Time out\n", k)
			v.state = _die
			if v.task == _map {
				master.rwLock.Lock()
				master.currentData = append(master.currentData, v.data...)
				master.splitDataNumber++
				delete(master.workers.workers, k)
				master.rwLock.Unlock()
			}
		}
	}
}

/**
Check worker alive routine
**/
func (master *_masterServer) checkWorkerHealth(ctx context.Context) {
	keepAliveTick := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Println("checkWorkerHealth Done")
		case <-keepAliveTick.C:
			master.checkWorkerAliveTimeOut()
		}
	}
}

/**
Give the worker Map task data
the data will store on the worker info which store on the master
**/
func (master *_masterServer) giveMapTaskToWorker(worker *worker) {
	master.rwLock.Lock()
	worker.data = master.currentData[0:master.splitSize]
	log.Printf("worker IP Address %s", worker.ipAddress)
	master.currentData = master.currentData[master.splitSize:len(master.currentData)]
	master.splitDataNumber--
	master.rwLock.Unlock()
	worker.state = _progress
	worker.task = _map
}

func (master *_masterServer) wokerReplyComplete(worker *worker) {
	log.Printf("IP %s complete %d", worker.ipAddress, worker.state)
	worker.state = _idle
	if worker.task == _map {
		_, ok := master.reduceDataURL[worker.ipAddress]
		if !ok {
			master.reduceDataURL[worker.ipAddress] = 1
		} else {
			master.reduceDataURL[worker.ipAddress]++
		}
	}
	// The map or reduce work done
	// if the map should return the data position for reduce download
	worker.task = _none
}

func (master *_masterServer) workerReplyIdle(worker *worker) {
	if master.splitDataNumber > 0 {
		log.Printf("Set the %s worker a Map task", worker.ipAddress)
		master.giveMapTaskToWorker(worker)
	} else if len(master.reduceDataURL) != 0 {
		//reduce work part
		log.Printf("Set the %s worker a Reduce task", worker.ipAddress)
		worker.task = _reduce
		worker.state = _progress
	}
}

/*
receive the worker keep alive
if the IP is not in the worker table add to the new worker
*/
func (master *_masterServer) KeepAlive(ctx context.Context, in *mpProtoc.WorkerState) (*mpProtoc.MasterRequest, error) {
	workIP := in.IPAddress
	log.Printf("rec %s worker keep alive", workIP)
	log.Printf("worker %s worker Status %d", workIP, in.WorkerState)
	workReplyState := in.WorkerState
	if _, ok := master.workers.workers[_ipAddress(workIP)]; !ok {
		newWork := worker{
			state:     _workerState(workReplyState),
			task:      _none,
			ipAddress: workIP,
		}
		master.rwLock.Lock()
		master.workers.workers[_ipAddress(workIP)] = newWork
		master.rwLock.Unlock()
	}
	master.rwLock.RLock()
	worker := master.workers.workers[_ipAddress(workIP)]
	master.rwLock.RUnlock()
	worker.lastAliveTime = time.Now()

	switch _workerState(workReplyState) {
	case _complete:
		master.wokerReplyComplete(&worker)
	case _idle:
		master.workerReplyIdle(&worker)
	}

	master.rwLock.Lock()
	master.workers.workers[_ipAddress(workIP)] = worker
	master.rwLock.Unlock()

	return &mpProtoc.MasterRequest{WorkerTask: int32(worker.task), ReduceTaskNumber: int32(len(master.workers.workers))}, nil
}

/**
Receive the MapReduce need to process data and Coustom map and Reduce function from the Client
**/
func (master *_masterServer) RecvMapReduceInfoToMaster(ctx context.Context, in *mpProtoc.MapReduceInfo) (*mpProtoc.Result, error) {

	if in.DataPath != nil {
		master.rwLock.Lock()
		master.dataPath = append(master.dataPath, in.DataPath...)
		master.rwLock.Unlock()
	}

	if len(in.MapReduceAppPath) != 0 {
		master.mapReduceAppPath = in.MapReduceAppPath
	}

	return &mpProtoc.Result{Result: "OK"}, nil
}

/**
Worker get the Map Data
**/
func (master *_masterServer) GetMapWorkData(workerInfo *mpProtoc.WorkerInfo, stream mpProtoc.MapReduceService_GetMapWorkDataServer) error {
	log.Printf("IP %s take the map Data", workerInfo.IPAddress)
	master.rwLock.RLock()
	sendData := master.workers.workers[_ipAddress(workerInfo.IPAddress)].data
	master.rwLock.RUnlock()
	stream.Send(&mpProtoc.MapData{Data: sendData})
	return nil
}

/**
Worker get the map reduce function
**/
func (master *_masterServer) DownloadMapReduceApp(workerInfo *mpProtoc.WorkerInfo, stream mpProtoc.MapReduceService_DownloadMapReduceAppServer) error {
	log.Printf("IP %s take the mapReduce map function and reduce function", workerInfo.IPAddress)
	data, err := ioutil.ReadFile(master.mapReduceAppPath)
	if err != nil {
		log.Println(err)
		return err
	}

	err = stream.Send(&mpProtoc.Chunk{Contents: data})

	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func (master *MpMaster) RunMaster(port string) error {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	master.masterServer.workers.workers = make(map[_ipAddress]worker)
	master.masterServer.reduceDataURL = make(map[string]int)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go master.masterServer.readDataRoutine(ctx)
	go master.masterServer.checkWorkerHealth(ctx)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("Server listening on", port)

	gRPCServer := grpc.NewServer()
	mpProtoc.RegisterMapReduceServiceServer(gRPCServer, &master.masterServer)
	if err := gRPCServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		return err
	}
	return nil
}
