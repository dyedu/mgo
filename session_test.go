package mgo

import (
	"fmt"
	"github.com/Centny/gwf/util"
	"gopkg.in/bson.v2"
	"testing"
	"time"
)

var CN_TEST = "test"

func TestCollection(t *testing.T) {
	//
	DialDbForOldVersion("loc.m:27017/test*8")

	{
		//test create index

		//test drop index
	}

	{
		//remove all
		info, err := C(CN_TEST).RemoveAll(nil)
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println(util.S2Json(info))

		count, err := C(CN_TEST).Count()
		if err != nil {
			t.Error(err)
			return
		}
		if count != 0 {
			t.Error("total count != 0 after remove all")
			return
		}
	}

	{
		//test insert
		err := C(CN_TEST).Insert(bson.M{"_id": bson.NewObjectId().Hex(), "test": "yes"})
		if err != nil {
			t.Error(err)
			return
		}

		//test insert 10001 documents
		var docs = []interface{}{}
		for i := 1; i <= 10001; i++ {
			docs = append(docs, bson.M{"_id": bson.NewObjectId().Hex(), "num": i})
		}
		err = C(CN_TEST).Insert(docs...)
		if err != nil {
			t.Error(err)
			return
		}
	}

	{
		//test update
		var id = bson.NewObjectId().Hex()
		err := C(CN_TEST).Insert(bson.M{"_id": id, "update": 0})
		if err != nil {
			t.Error(err)
			return
		}

		err = C(CN_TEST).Update(
			bson.M{"_id": id},
			bson.M{
				"$inc": bson.M{"update": 1},
				"$set": bson.M{"setValue": "value"},
			},
		)
		if err != nil {
			t.Error(err)
			return
		}

		err = C(CN_TEST).UpdateId(id, bson.M{
			"$inc": bson.M{"update": 1},
			"$set": bson.M{"setValue": "value"},
		})
		if err != nil {
			t.Error(err)
			return
		}

		err = C(CN_TEST).Update(bson.M{"_id": id, "update": 0}, bson.M{"$set": bson.M{"setValue": "update error"}})
		fmt.Println("id", id, "error -> ", err)
		if err == nil {
			t.Error("update not found documents but no errors occurred")
			return
		}

		//test update all
		var ids = []string{}
		for i := 0; i < 10; i++ {
			ids = append(ids, bson.NewObjectId().Hex())
			err := C(CN_TEST).Insert(bson.M{"_id": ids[i], "update": 0})
			if err != nil {
				t.Error(err)
				return
			}
		}

		info, err := C(CN_TEST).UpdateAll(bson.M{"_id": bson.M{"$in": ids}, "update": 0}, bson.M{"$set": bson.M{"update": 10000}})
		if err != nil {
			t.Error(err)
			return
		}

		info, err = C(CN_TEST).UpdateAll(bson.M{"_id": bson.M{"$in": ids}, "update": 0}, bson.M{"$set": bson.M{"update": 10000}})
		if err != nil {
			t.Error(err)
			return
		}

		//test upsert
		id = bson.NewObjectId().Hex()
		info, err = C(CN_TEST).Upsert(bson.M{"_id": id}, bson.M{"$set": bson.M{"set": true}})
		if err != nil {
			t.Error(err)
			return
		}
		if info.UpsertedId == nil {
			t.Error("upsert id is null")
			return
		}

		info, err = C(CN_TEST).UpsertId(id, bson.M{"$set": bson.M{"set": true}})
		if err != nil {
			t.Error(err)
			return
		}
		if info.UpsertedId != nil {
			t.Error("upsert id is not null", info.UpsertedId)
			return
		}

	}

	{
		//test query
		C(CN_TEST).RemoveAll(nil)

		var docs = []interface{}{}
		for i := 1; i <= 200; i++ {
			docs = append(docs, bson.M{"_id": i, "const": 0, "var": i, "mod": i % 10})
		}

		err := C(CN_TEST).Insert(docs...)
		if err != nil {
			t.Error(err)
			return
		}

		var idRecord = map[interface{}]int{}
		//test query all with skip limit
		for i := 0; i < 200; i += 10 {
			var result = []map[string]interface{}{}
			err := C(CN_TEST).Find(bson.M{"const": 0}).Sort("const", "-_id").Select(bson.M{"_id": 1}).Skip(i).Limit(10).All(&result)
			if err != nil {
				t.Error(err)
				return
			}
			fmt.Println(util.S2Json(result))

			for _, item := range result {
				if idRecord[item["_id"]] > 0 {
					t.Error("repeat id")
					return
				}
				idRecord[item["_id"]] = 1
			}
		}

		//test query one
		idRecord = map[interface{}]int{}
		for i := 0; i < 200; i++ {
			var result = map[string]interface{}{}
			err := C(CN_TEST).Find(bson.M{"const": 0}).Sort("-id").Skip(i).One(&result)
			if err != nil {
				t.Error(err)
				return
			}

			if idRecord[result["_id"]] > 0 {
				t.Error("repeat id")
				return
			}
			idRecord[result["_id"]] = 1
		}

		//test find and modify
		var tmp interface{}
		var id = bson.NewObjectId().Hex()
		info, err := C(CN_TEST).Find(bson.M{"_id": id}).Apply(Change{
			Upsert: true,
			Update: bson.M{"$set": bson.M{"value": 1}},
		}, &tmp)
		if err != nil {
			t.Error(err)
			return
		}
		if info.UpsertedId == nil || info.Matched != 0 || info.Updated != 1 {
			t.Error("info err", util.S2Json(info))
			return
		}

		info, err = C(CN_TEST).Find(bson.M{"_id": id}).Apply(Change{
			Upsert: true,
			Update: bson.M{"$set": bson.M{"value": 2}},
		}, &tmp)
		if err != nil {
			t.Error(err)
			return
		}
		if info.UpsertedId != nil || info.Matched != 1 || info.Updated != 1 {
			t.Error("repeat findAndModify the same _id, but info err", util.S2Json(info))
			return
		}

		info, err = C(CN_TEST).Find(bson.M{"_id": id, "value": 3}).Apply(Change{
			Upsert: false,
			Update: bson.M{"$set": bson.M{"value": 2}},
		}, &tmp)
		if err.Error() != ErrNotFound.Error() {
			t.Error(err)
			return
		}

		//test find and modify returnNew
		var data = map[string]interface{}{}
		info, err = C(CN_TEST).Find(bson.M{"_id": id}).Apply(Change{
			Upsert:    true,
			Update:    bson.M{"$set": bson.M{"value": 3}},
			ReturnNew: true,
		}, &data)
		if err != nil {
			t.Error(err)
			return
		}
		if data["value"] != 3 {
			t.Error("return new option err", util.S2Json(data))
			return
		}

		info, err = C(CN_TEST).Find(bson.M{"_id": id}).Apply(Change{
			Upsert:    true,
			Update:    bson.M{"$set": bson.M{"value": 4}},
			ReturnNew: false,
		}, &data)
		if err != nil {
			t.Error(err)
			return
		}
		if data["value"] != 3 {
			t.Error("return new option err", util.S2Json(data))
			return
		}

		//idRecord = map[interface{}]int{}
		//for i := 0; i < 200; i++ {
		//	var result = map[string]interface{}{}
		//	info, err := C(CN_TEST).Find(bson.M{"const": 0}).Sort("-id").Skip(i).Apply(Change{
		//		Update:    bson.M{"newconst": 1},
		//		Upsert:    false,
		//		ReturnNew: false,
		//	}, &result)
		//	if err != nil {
		//		t.Error(err)
		//		return
		//	}
		//	fmt.Println(util.S2Json(info))
		//	//if info.Updated != 1 {
		//	//	t.Error()
		//	//}
		//	if idRecord[result["_id"]] > 0 {
		//		t.Error("repeat id", i)
		//		return
		//	}
		//	if result["newconst"] != nil {
		//		t.Error("ReturnNew param is useless")
		//		return
		//	}
		//	idRecord[result["_id"]] = 1
		//}

		//test find one
		err = C(CN_TEST).Find(bson.M{"_id": 99999}).One(&tmp)
		if err.Error() != ErrNotFound.Error() {
			t.Error("not found but no errors occurred", err)
			return
		}

		//test remove
		for _, value := range idRecord {
			err = C(CN_TEST).RemoveId(value)
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	{
		//test pipe
		C(CN_TEST).RemoveAll(nil)

		var docs = []interface{}{}
		for i := 1; i <= 200; i++ {
			docs = append(docs, bson.M{"_id": i, "const": 0, "var": i, "mod": i % 10})
		}

		err := C(CN_TEST).Insert(docs...)
		if err != nil {
			t.Error(err)
			return
		}

		var result = []map[string]interface{}{}
		pipe := []bson.M{
			{
				"$match": bson.M{"const": 0},
			}, {
				"$sort": bson.D{{"const", 1}, {"var", -1}},
			}, {
				"$limit": 100,
			},
		}
		err = C(CN_TEST).Pipe(pipe).All(&result)
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println(util.S2Json(result))
		if len(result) != 100 {
			t.Error("aggregate result error")
			return
		}

		var one = map[string]interface{}{}
		err = C(CN_TEST).Pipe(pipe).One(&one)
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println(util.S2Json(one))
		if len(one) == 0 {
			t.Error("aggregate result error")
			return
		}

		err = C(CN_TEST).Pipe([]bson.M{{"$match": bson.M{"test": "Abc"}}}).One(&one)
		if err.Error() != ErrNotFound.Error() {
			t.Error("not found but no errors occurred", err)
			return
		}
	}

	{
		//test bulk
		var id = bson.NewObjectId().Hex()
		bulk := C(CN_TEST).Bulk()
		bulk.Insert(bson.M{"_id": id})
		bulk.Update(bson.M{"_id": id}, bson.M{"$set": bson.M{"value": 1}})
		bulk.Update(bson.M{"_id": id}, bson.M{"$set": bson.M{"value2": 2}})
		bulk.Remove(bson.M{"_id": id})
		result, err := bulk.Run()
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Println(util.S2Json(result))

		var start = time.Now().UnixNano()
		//test over 1000 opts
		bulk = C(CN_TEST).Bulk()
		for i := 1; i <= 1001; i++ {
			var id = bson.NewObjectId().Hex()
			bulk.Insert(bson.M{"_id": id})
			bulk.Update(bson.M{"_id": id}, bson.M{"$set": bson.M{"value": 1}})
		}
		result, err = bulk.Run()
		if err != nil {
			t.Error(err)
			return
		}
		var end = time.Now().UnixNano() - start
		fmt.Println(util.S2Json(result), end/1e6, "ms")

	}
}

func TestMock(t *testing.T) {
	Mock = true

	DialDb("loc.m:27017", "test", 10, 1)

	var tmp interface{}

	SetMckC("Query-One", 0)
	err := C(CN_TEST).Find(bson.M{}).One(&tmp)
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Query-All", 0)
	err = C(CN_TEST).Find(bson.M{}).All(&tmp)
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Query-Distinct", 0)
	err = C(CN_TEST).Find(bson.M{}).Distinct("_id", &tmp)
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Query-Count", 0)
	_, err = C(CN_TEST).Find(bson.M{}).Count()
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-Update", 0)
	err = C(CN_TEST).Update(bson.M{"_id": 1}, bson.M{"s": 1})
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-UpdateId", 0)
	err = C(CN_TEST).UpdateId(1, bson.M{"s": 1})
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-UpdateAll", 0)
	_, err = C(CN_TEST).UpdateAll(bson.M{"_id": 1}, bson.M{"s": 1})
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-Upsert", 0)
	_, err = C(CN_TEST).Upsert(bson.M{"_id": 1}, bson.M{"s": 1})
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-UpsertId", 0)
	_, err = C(CN_TEST).UpsertId(bson.M{"_id": 1}, bson.M{"s": 1})
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-Remove", 0)
	err = C(CN_TEST).Remove(bson.M{"_id": 1})
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-RemoveId", 0)
	err = C(CN_TEST).RemoveId(bson.M{"_id": 1})
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-RemoveAll", 0)
	_, err = C(CN_TEST).RemoveAll(bson.M{"_id": 1})
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Collection-Count", 0)
	_, err = C(CN_TEST).Count()
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Pipe-One", 0)
	err = C(CN_TEST).Pipe([]bson.M{}).One(&tmp)
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Pipe-All", 0)
	err = C(CN_TEST).Pipe([]bson.M{}).All(&tmp)
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()

	SetMckC("Bulk-Run", 0)
	bulk := C(CN_TEST).Bulk()
	_, err = bulk.Run()
	if err.Error() != MockError.Error() {
		t.Error("had set mock value but no mock errors occurred")
		return
	}
	ClearMock()
}

func TestAA(t *testing.T) {
	DialDb("loc.m:27017", "test", 10, 1)
	C(CN_TEST).RemoveAll(nil)

	var docs = []interface{}{}
	for i := 1; i <= 200; i++ {
		docs = append(docs, bson.M{"_id": i, "const": 0, "var": i, "mod": i % 10})
	}

	err := C(CN_TEST).Insert(docs...)
	if err != nil {
		t.Error(err)
		return
	}

	var ids = []string{}

	err = C(CN_TEST).Find(bson.M{}).Distinct("_id", ids)
	//data, err := C(CN_TEST).Find(bson.M{}).Distinct2("_id", &ids)
	//fmt.Println(data)
	fmt.Println(ids)
	fmt.Println(err)
	//fmt.Println(fmt.Sprintf("%p, %p", &data.Values, &ids))
}

func TestFindAll(t *testing.T) {
	DialDb("loc.m:27017", "test", 10, 1)
	C(CN_TEST).RemoveAll(nil)

	var docs = []interface{}{}
	for i := 1; i <= 200; i++ {
		docs = append(docs, bson.M{"_id": i, "const": 0, "var": i, "mod": i % 10})
	}

	err := C(CN_TEST).Insert(docs...)
	if err != nil {
		t.Error(err)
		return
	}

	var tmps = []util.Map{}
	err = C(CN_TEST).Find(bson.M{}).Limit(10).All(&tmps)
	if err != nil {
		t.Error(err)
		return
	}
	if len(tmps) != 10 {
		t.Error("length != 10", len(tmps))
		return
	}

	tmps = []util.Map{}
	err = C(CN_TEST).Find(bson.M{}).Skip(10).Limit(10).All(&tmps)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(util.S2Json(tmps))
	if len(tmps) != 10 {
		t.Error("length != 10", len(tmps))
		return
	}

	err = C(CN_TEST).Find(bson.M{}).Skip(20).Limit(10).All(&tmps)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(util.S2Json(tmps))
	if len(tmps) != 20 {
		t.Error("length != 20", len(tmps))
		return
	}
}

//func TestConnectTwoDb(t *testing.T) {
//	session := DialDbForOldVersion("cny:123@loc.m:27017/test*8")
//	err := session.C("test").Insert(bson.M{"_id":bson.NewObjectId().Hex()})
//	count, err := session.C("test").Count()
//	fmt.Println(count, err)
//
//	session2 := DialNewDbForOldVersion("192.168.2.37:27017/test*8")
//	count, err = session2.C("test").Count()
//	fmt.Println(count, err)
//
//	count, err = session.C("test").Count()
//	fmt.Println(count, err)
//
//}
