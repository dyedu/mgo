package mgo

import (
	"errors"
	"fmt"
	"github.com/Centny/gwf/tutil"
)

var Mock = false
var MckL = false
var MckC = map[string]int{}
var mckc = map[string]int{}
var MckV = map[string]interface{}{}
var MockError = errors.New("Mock Error")

func SetMckC(key string, c int) {
	MckC[key] = c
}

func SetMckV(key string, c int, v interface{}) {
	MckC[key] = c
	MckV[key] = v
}

func chk_mock(key string) bool {
	tc, ok := MckC[key]
	if MckL {
		fmt.Printf("Mock->%v by matched(%v),current(%v),mock(%v)\n", key, ok, tc, mckc[key])
	}
	if ok {
		eq := tc == mckc[key]
		mckc[key] += 1
		return eq
	} else {
		return false
	}
}

func ClearMock() {
	MckC = map[string]int{}
	mckc = map[string]int{}
	MckV = map[string]interface{}{}
}

var M *tutil.Monitor = tutil.NewMonitor()
