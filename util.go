package mgo

import (
	"github.com/Centny/gwf/util"
	"gopkg.in/bson.v2"
	"math"
	"sort"
)

const CN_SEQUENCE = "sequence"

func Next(key string, increase int) (string, int64, error) {
	var data = util.Map{}
	_, err := C(CN_SEQUENCE).Find(bson.M{"_id": key}).Apply(Change{
		Update:    bson.M{"$inc": bson.M{"val": increase}},
		Upsert:    true,
		ReturnNew: true,
	}, &data)
	if err != nil {
		return "", 0, err
	}
	return data.StrVal("val"), data.IntVal("val"), err
}



type SortD struct {
	bson.D
	Desc bool
}

func (s SortD) Len() int {
	return len(s.D)
}

func (s SortD) Less(i, j int) bool {
	if s.Desc {
		return math.Abs(util.FloatVal(s.D[i].Value)) > math.Abs(util.FloatVal(s.D[j].Value))
	}
	return math.Abs(util.FloatVal(s.D[i].Value)) < math.Abs(util.FloatVal(s.D[j].Value))
}

func (s SortD) Swap(i, j int) {
	s.D[i], s.D[j] = s.D[j], s.D[i]
}

func (s SortD) Val() bson.D {
	var val = bson.D{}
	for _, v := range s.D {
		if util.IntVal(v.Value) > 0 {
			val = append(val, bson.DocElem{
				Name:  v.Name,
				Value: 1,
			})
		} else {
			val = append(val, bson.DocElem{
				Name:  v.Name,
				Value: -1,
			})
		}
	}
	return val
}

func ParseSortD(msort util.Map) bson.D {
	var sd = SortD{}
	for key := range msort {
		sd.D = append(sd.D, bson.DocElem{
			Name:  key,
			Value: msort.IntVal(key),
		})
	}
	sort.Sort(sd)
	return sd.Val()
}
