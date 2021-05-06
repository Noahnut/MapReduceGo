package main

import "github.com/Noahnut/mapReduce/mpClient"

func main() {
	client := mpClient.MpClient{}
	dataPath := make([]string, 0)
	dataPath = append(dataPath, "testOne")
	client.StartMapReduce("5553", dataPath, "./mapReduceApp/wordCount.so")
}
