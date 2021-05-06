package mpMaster

import (
	"io/ioutil"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\n")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
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
		var stringArray string
		for i := 0; i < e.fileSize; i++ {
			value := strconv.Itoa(i)
			stringArray += value
			stringArray += " "
			if i%20 == 0 {
				stringArray += "\n"
			}
		}
		ioutil.WriteFile(e.fileName, []byte(stringArray), 0664)
	}
}
