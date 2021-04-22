package main

import mpWoker "github.com/Noahnut/mapReduce/mpWorker"

func main() {
	worker := mpWoker.MpWorker{}
	worker.CreateMpWorker("localhost", "127.0.0.2", "5553")
}
