package mpWoker

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"plugin"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Noahnut/mapReduce/mpProtoc"
	"google.golang.org/grpc"
)

type MapFun func([]byte) []string
type ReduceFun func(string, []string) string

type _workerState int
type _workerTask int
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

const mapReduceAppFileName = "MapReduceApp.so"

type KeyValue struct {
	Key   string
	Value string
}

type MpWorker struct {
	_mpWorkerClient   mpProtoc.MapReduceServiceClient
	_mpData           []byte
	_workerIP         string
	_workState        _workerState
	_workTask         _workerTask
	_mapFunc          MapFun
	_reduceFun        ReduceFun
	_reduceTaskNumber int
}

func (worker *MpWorker) ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (worker *MpWorker) fileExists(fileName string) error {
	_, err := os.Stat(fileName)

	if os.IsNotExist(err) {
		_, err := os.Create(fileName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (worker *MpWorker) loadPlugin() bool {
	p, err := plugin.Open(mapReduceAppFileName)
	if err != nil {
		log.Println(err)
		return false
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Printf("cannot find Map in %v", mapReduceAppFileName)
		return false
	}
	worker._mapFunc = xmapf.(func([]byte) []string)

	xreducef, err := p.Lookup("Reduce")

	if err != nil {
		log.Printf("cannot find Reduce in %v", mapReduceAppFileName)
		return false
	}
	worker._reduceFun = xreducef.(func(string, []string) string)
	return true
}

func (worker *MpWorker) getMapReducdeApp() bool {
	file, err := os.Create(mapReduceAppFileName)
	if err != nil {
		log.Println(err)
		return false
	}
	defer file.Close()

	reply, err := worker._mpWorkerClient.DownloadMapReduceApp(context.Background(), &mpProtoc.WorkerInfo{IPAddress: worker._workerIP})
	if err != nil {
		log.Println(err)
	} else {
		data, err := reply.Recv()
		if err != nil {
			log.Println(err)
			return false
		}
		file.Write(data.Contents)
	}
	return true
}

func (worker *MpWorker) GetMapData() bool {
	worker._workState = _progress
	reply, err := worker._mpWorkerClient.GetMapWorkData(context.Background(), &mpProtoc.WorkerInfo{IPAddress: worker._workerIP})
	if err != nil {
		log.Println(err)
	} else {
		data, err := reply.Recv()
		if err != nil {
			log.Println(err)
		} else if len(data.Data) == 0 {
			log.Println("Data is empty")
		} else {
			worker._mpData = data.Data
			worker._workState = _progress
			return true
		}
	}
	return false
}

func (worker *MpWorker) generateIntermediateData(keyValue []string, reduceTaskNumber int) [][]KeyValue {
	intermediate := make([][]KeyValue, reduceTaskNumber)
	for _, keyvalueString := range keyValue {
		splitData := strings.Split(keyvalueString, ":")
		keyvalue := KeyValue{Key: splitData[0], Value: splitData[1]}
		buctetIndex := worker.ihash(keyvalue.Key) % (reduceTaskNumber)
		intermediate[buctetIndex] = append(intermediate[buctetIndex], keyvalue)
	}
	return intermediate
}

func (worker *MpWorker) writeIntermediateDataToFileAsJsonType(intermediate [][]KeyValue) error {
	for i, interintermediateData := range intermediate {
		fileName := worker._workerIP + "_" + strconv.Itoa(i) + ".json"

		err := worker.fileExists(fileName)

		if err != nil {
			return err
		}

		file, err := ioutil.ReadFile(fileName)
		if err != nil {
			return err
		}

		existData := []KeyValue{}
		json.Unmarshal(file, &existData)
		existData = append(existData, interintermediateData...)
		sort.Slice(existData, func(i, j int) bool {
			if existData[i].Key < existData[j].Key {
				return true
			} else {
				return false
			}
		})

		byteData, err := json.Marshal(&existData)

		if err != nil {
			return err
		}

		err = ioutil.WriteFile(fileName, byteData, 0644)

		if err != nil {
			return err
		}
	}
	return nil
}

func (worker *MpWorker) mapTask() {
	log.Println(worker._workerIP)
	keyValue := worker._mapFunc(worker._mpData)
	currentReduceTaskNumber := worker._reduceTaskNumber
	intermediate := worker.generateIntermediateData(keyValue, currentReduceTaskNumber)
	err := worker.writeIntermediateDataToFileAsJsonType(intermediate)
	if err != nil {
		log.Println(err)
		return
	}
	worker._workState = _complete
}

func (worker *MpWorker) reduceTask() {
	worker._workState = _progress
}

func (worker *MpWorker) startTask() {
	if worker._workState == _idle && worker._workTask != _workerTask(_none) {
		if worker._workTask == _workerTask(_map) {
			if !worker.GetMapData() {
				worker._workState = _die
			} else {
				worker.mapTask()
			}
		} else if worker._workTask == _workerTask(_reduce) {
			worker.reduceTask()
		}
	} else if worker._workState == _complete && worker._workTask == _workerTask(_none) {
		worker._workState = _idle
	}
}

func (worker *MpWorker) checkCurrentStateAndTask(ctx context.Context) {
	keepAliveTick := time.NewTicker(2 * time.Second)
	aliveAgain := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Println("checkCurrentStateAndTask Exit")
		case <-keepAliveTick.C:
			worker.startTask()
		case <-aliveAgain.C:
			if worker._workState == _die {
				worker._workState = _idle
			}
		}
	}
}

func (worker *MpWorker) keepAlive(ctx context.Context) {
	log.Printf("IP %s Start Send keepAlive", worker._workerIP)
	keepAliveTick := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Println("keepAlive exit")
		case <-keepAliveTick.C:
			reply, err := worker._mpWorkerClient.KeepAlive(context.Background(), &mpProtoc.WorkerState{IPAddress: worker._workerIP, WorkerState: int32(worker._workState)})
			if err != nil {
				log.Println(err)
			} else {
				worker._workTask = _workerTask(reply.WorkerTask)
				worker._reduceTaskNumber = int(reply.ReduceTaskNumber)
			}
		}
	}
}

func (worker *MpWorker) wokerMain(ctx context.Context) {
	worker.getMapReducdeApp()
	if !worker.loadPlugin() {
		log.Fatalln("Can not load the map reduce function")
	}
	go worker.keepAlive(ctx)
	go worker.checkCurrentStateAndTask(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Println("worker Main exit")
		}
	}
}

func (worker *MpWorker) CreateMpWorker(masterIP string, workerIP string, port string) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var wg sync.WaitGroup

	conn, err := grpc.Dial(masterIP+":"+port, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Can not create grpc error: %v", err)
	}
	defer conn.Close()
	worker._mpWorkerClient = mpProtoc.NewMapReduceServiceClient(conn)
	worker._workerIP = workerIP
	worker._workState = _idle
	worker._workTask = _workerTask(_none)
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	wg.Add(1)
	go worker.wokerMain(ctx)
	wg.Wait()
}
