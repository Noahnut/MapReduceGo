package main

import mpMaster "github.com/Noahnut/mapReduce/mpMaster"

func main() {
	master := mpMaster.MpMaster{}
	testArray := []string{"test"}
	master.RunMaster("5553", testArray)
}
