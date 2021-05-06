package main

import mpMaster "github.com/Noahnut/mapReduce/mpMaster"

func main() {
	master := mpMaster.MpMaster{}
	master.RunMaster("5553")
}
