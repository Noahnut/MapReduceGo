package main

import (
	"sync"

	mpWoker "github.com/Noahnut/mapReduce/mpWorker"
)

func Worker(IPaddress string) {
	worker := mpWoker.MpWorker{}

	worker.CreateMpWorker("localhost", IPaddress, "5553")

}

func main() {
	var wg sync.WaitGroup
	wg.Add(5)
	go Worker("127.0.0.1")
	go Worker("127.0.0.2")
	go Worker("127.0.0.3")
	go Worker("127.0.0.4")
	go Worker("127.0.0.5")
	wg.Wait()
}
