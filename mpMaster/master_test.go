package mpMaster

import (
	"io/ioutil"
	"os"
	"testing"
)

func Test_readDataFromPath(t *testing.T) {
	testCase := []struct {
		fileName          string
		fileSize          int
		expectDataSize    int
		expectSplitNumber int
	}{
		{fileName: "./testOne", fileSize: 1024, expectDataSize: 64, expectSplitNumber: 16},
		{fileName: "./testTwo", fileSize: 3 * KB, expectDataSize: 64, expectSplitNumber: 48},
		{fileName: "./testThree", fileSize: 3 * MB, expectDataSize: 64 * KB, expectSplitNumber: 48},
	}

	for _, e := range testCase {
		mpMaster := _masterServer{}
		TestByte := make([]byte, e.fileSize)
		ioutil.WriteFile(e.fileName, TestByte, 0664)
		mpMaster.dataPath = append(mpMaster.dataPath, e.fileName)
		mpMaster.readDataFromPath()
		if mpMaster.splitDataNumber != e.expectSplitNumber {
			t.Errorf("The splitDataNumber %d not equal expect %d", mpMaster.splitDataNumber, e.expectSplitNumber)
		}

		if mpMaster.splitSize != e.expectDataSize {
			t.Errorf("The splitDataSize %d not equal expect %d", mpMaster.splitSize, e.expectDataSize)

		}
		os.Remove(e.fileName)
	}
}
