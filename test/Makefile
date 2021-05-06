
Test:
	go build -race WorkerMain.go
	go build -race MasterMain.go
	go build -race ClientMain.go
	go build -race -buildmode=plugin ./mapReduceApp/wordCount.go
	rm *.json