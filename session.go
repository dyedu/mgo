package mgo

import (
	"fmt"
	"github.com/Centny/gwf/log"
	"github.com/Centny/gwf/util"
	"gopkg.in/bson.v2"
	"gopkg.in/mongoc.v1"
	"strings"
	"time"
)

func init() {
	mongoc.LogHandler = DefaultLogHandler
}

//DefaultLogHandler default mongoc log handler. impl by log.Printf("[level] domain:message")
func DefaultLogHandler(logLevel mongoc.LogLevel, logDomain, message string) {
	switch logLevel {
	case mongoc.LogLevelError:
		log.SharedLogger().Output(3, fmt.Sprintf("E %v:%v", logDomain, message))
	case mongoc.LogLevelCritical:
		log.SharedLogger().Output(3, fmt.Sprintf("C %v:%v", logDomain, message))
	case mongoc.LogLevelWarning:
		log.SharedLogger().Output(3, fmt.Sprintf("W %v:%v", logDomain, message))
	case mongoc.LogLevelMessage:
		log.SharedLogger().Output(3, fmt.Sprintf("M %v:%v", logDomain, message))
	case mongoc.LogLevelInfo:
		log.SharedLogger().Output(3, fmt.Sprintf("I %v:%v", logDomain, message))
	case mongoc.LogLevelDebug:
		log.SharedLogger().Output(3, fmt.Sprintf("D %v:%v", logDomain, message))
	case mongoc.LogLevelTrace:
		log.SharedLogger().Output(3, fmt.Sprintf("T %v:%v", logDomain, message))
	default:
		log.SharedLogger().Output(3, fmt.Sprintf("U %v:%v", logDomain, message))
	}
}

var ErrNotFound = mongoc.ErrNotFound

var C func(string) *Collection
var Execute func(cmds, v interface{}) error
var pool *mongoc.Pool

func selectParam(url string) (string, string, uint32, uint32) {
	var dbName, uri string
	var maxCollectionCount uint32 = 1
	var idleCollectionCount uint32 = 1
	var array = strings.Split(url, "/")
	uri = array[0]
	if len(array) > 1 {
		var params = strings.Split(array[1], "*")
		dbName = params[0]
		if len(params) > 1 {
			maxCollectionCount = uint32(util.UintVal(params[1]))
			if maxCollectionCount == 0 {
				panic(fmt.Sprintf("receive url(%v) when use (%v) convert to int error", url, params[1]))
			}
		}
	}
	if len(array) > 2 {
		uri = uri + "/?" + array[2]
	}
	return uri, dbName, maxCollectionCount, idleCollectionCount
}

//eg:	cny:123@192.168.2.19:27017/cny*8/?authMechanism=SCRAM-SHA-1&authSource=mydb
func DialDbForOldVersion(url string) *Session {
	uri, dbName, maxCollectionCount, idleCollectionCount := selectParam(url)
	pool = mongoc.NewPool(uri, maxCollectionCount, idleCollectionCount)
	log.D("Dial mongodb by uri(%v)", uri)
	C = func(collectionName string) *Collection {
		return &Collection{
			collection: pool.C(dbName, collectionName),
		}
	}
	Execute = func(cmds, v interface{}) error {
		var mid = M.Start("Db-Execute")
		defer M.Done(mid)
		if Mock && chk_mock("Db-Execute") {
			return MockError
		}
		return pool.Execute(dbName, cmds, nil, v)
	}
	return &Session{pool: pool, C: C}
}

func DialDb(uri, dbName string, maxSize, minSize uint32) *Session {
	pool = mongoc.NewPool(uri, maxSize, minSize)
	C = func(collectionName string) *Collection {
		return &Collection{
			collection: pool.C(dbName, collectionName),
		}
	}
	return &Session{pool: pool, C: C}
}

func DialNewDbForOldVersion(url string) *Session {
	uri, dbName, maxCollectionCount, idleCollectionCount := selectParam(url)
	var newPool = mongoc.NewPool(uri, maxCollectionCount, idleCollectionCount)
	log.D("Dial mongodb by uri(%v) new pool", uri)
	return &Session{pool: newPool, C: func(collectionName string) *Collection {
		return &Collection{
			collection: newPool.C(dbName, collectionName),
		}
	}}
}

type Session struct {
	pool *mongoc.Pool
	C    func(string) *Collection
}

type ChangeInfo struct {
	// Updated reports the number of existing documents modified.
	// Due to server limitations, this reports the same value as the Matched field when
	// talking to MongoDB <= 2.4 and on Upsert and Apply (findAndModify) operations.
	Updated    int
	Removed    int         // Number of documents removed
	Matched    int         // Number of documents matched but not necessarily changed
	UpsertedId interface{} // Upserted _id field, when not explicitly provided
}

func NewChangeInfo(changed *mongoc.Changed) *ChangeInfo {
	if changed == nil {
		return nil
	}
	return &ChangeInfo{
		Updated:    changed.Updated,
		UpsertedId: changed.Upserted,
		Matched:    changed.Matched,
	}
}

func EnsureIndexes(C func(string) *Collection, indexes map[string][]*mongoc.Index) error {
	for collectionName, data := range indexes {
		if len(data) > 0 {
			err := C(collectionName).EnsureIndexes(data...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type Collection struct {
	collection *mongoc.Collection
}

func (c *Collection) DropCollection() error {
	return c.collection.Drop()
}

func (c *Collection) DropIndex(key ...string) error {
	return nil
}

func (c *Collection) DropIndexName(name string) error {
	return c.collection.DropIndexes(name)
}

func (c *Collection) EnsureIndexes(indexes ...*mongoc.Index) error {
	return c.collection.CreateIndexes(indexes...)
}

func (c *Collection) ListIndexes() ([]*mongoc.Index, error) {
	return c.collection.ListIndexes()
}

func (c *Collection) Insert(documents ...interface{}) error {
	var mid = M.Start("Collection-Insert")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-Insert") {
		return MockError
	}
	return c.collection.Insert(documents...)
}

func (c *Collection) Update(selector, update interface{}) error {
	var mid = M.Start("Collection-Update")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-Update") {
		return MockError
	}
	return c.collection.UpdateOne(selector, update)
}

func (c *Collection) UpdateId(id interface{}, update interface{}) error {
	var mid = M.Start("Collection-UpdateId")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-UpdateId") {
		return MockError
	}
	return c.Update(bson.D{{"_id", id}}, update)
}

func (c *Collection) UpdateAll(selector, update interface{}) (*ChangeInfo, error) {
	var mid = M.Start("Collection-UpdateAll")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-UpdateAll") {
		return nil, MockError
	}
	changed, err := c.collection.UpdateMany(selector, update)
	return NewChangeInfo(changed), err
}

func (c *Collection) Upsert(selector, update interface{}) (*ChangeInfo, error) {
	var mid = M.Start("Collection-Upsert")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-Upsert") {
		return nil, MockError
	}
	info, err := c.collection.Upsert(selector, update)
	return NewChangeInfo(info), err
}

func (c *Collection) UpsertId(id, update interface{}) (*ChangeInfo, error) {
	var mid = M.Start("Collection-UpsertId")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-UpsertId") {
		return nil, MockError
	}
	return c.Upsert(bson.D{{"_id", id}}, update)
}

func (c *Collection) Remove(selector interface{}) error {
	var mid = M.Start("Collection-Remove")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-Remove") {
		return MockError
	}
	_, err := c.collection.Remove(selector, true)
	return err
}

func (c *Collection) RemoveId(id interface{}) error {
	var mid = M.Start("Collection-RemoveId")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-RemoveId") {
		return MockError
	}
	return c.Remove(bson.D{{"_id", id}})
}

func (c *Collection) RemoveAll(selector interface{}) (*ChangeInfo, error) {
	var mid = M.Start("Collection-RemoveAll")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-RemoveAll") {
		return nil, MockError
	}
	n, err := c.collection.Remove(selector, false)
	return &ChangeInfo{Removed: n, Matched: n}, err
}

func (c *Collection) Count() (int, error) {
	var mid = M.Start("Collection-Count")
	defer M.Done(mid)
	if Mock && chk_mock("Collection-Count") {
		return 0, MockError
	}
	return c.Find(nil).Count()
}

func (c *Collection) Find(query interface{}) *Query {
	return &Query{
		query:      query,
		collection: c.collection,
	}
}

func (c *Collection) FindId(id interface{}) *Query {
	return c.Find(bson.D{{"_id", id}})
}

func (c *Collection) Pipe(pipeline interface{}) *Pipe {
	return &Pipe{
		pipeLine:   pipeline,
		collection: c.collection,
	}
}

func (c *Collection) Bulk() *Bulk {
	return &Bulk{
		collection: c.collection,
		bulk:       c.collection.NewBulk(true),
	}
}

type Query struct {
	collection  *mongoc.Collection
	query       interface{}
	selector    interface{}
	skip, limit int
	maxScan     int
	orderBy     bson.D
	maxDuration int
	hint        string
}

type Change struct {
	Update    interface{} // The update document
	Upsert    bool        // Whether to insert in case the document isn't found
	Remove    bool        // Whether to remove the document found rather than updating
	ReturnNew bool        // Should the modified document be returned rather than the old one
}

func (q *Query) mixCondition() interface{} {
	var result = bson.M{}
	if len(q.orderBy) > 0 {
		result["$orderby"] = q.orderBy
	}
	if q.maxScan > 0 {
		result["$maxScan"] = q.maxScan
	}
	if q.maxDuration > 0 {
		result["$maxTimeMS"] = q.maxDuration
	}
	if len(q.hint) > 0 {
		result["$hint"] = q.hint
	}
	if len(result) > 0 {
		result["$query"] = q.query
	} else {
		return q.query
	}
	return result
}

func (q *Query) Count() (int, error) {
	var mid = M.Start("Query-Count")
	defer M.Done(mid)
	if Mock && chk_mock("Query-Count") {
		return 0, MockError
	}
	return q.collection.Count(q.query, q.skip, q.limit)
}

func (q *Query) One(result interface{}) error {
	var mid = M.Start("Query-One")
	defer M.Done(mid)
	if Mock && chk_mock("Query-One") {
		return MockError
	}
	return q.collection.Find(q.mixCondition(), q.selector, q.skip, 1, result)
}

func (q *Query) All(result interface{}) error {
	var mid = M.Start("Query-All")
	defer M.Done(mid)
	if Mock && chk_mock("Query-All") {
		return MockError
	}
	return q.collection.Find(q.mixCondition(), q.selector, q.skip, q.limit, result)
}

func (q *Query) Select(selector interface{}) *Query {
	q.selector = selector
	return q
}

func (q *Query) Distinct(key string, result interface{}) error {
	var mid = M.Start("Query-Distinct")
	defer M.Done(mid)
	if Mock && chk_mock("Query-Distinct") {
		return MockError
	}
	return q.collection.Distinct(key, q.query, result)
}

func (q *Query) Sort(sortKeys ...string) *Query {
	q.orderBy = bson.D{}
	for _, k := range sortKeys {
		if strings.HasPrefix(k, "-") {
			q.orderBy = append(q.orderBy, bson.DocElem{Name: strings.TrimPrefix(k, "-"), Value: -1})
		} else {
			q.orderBy = append(q.orderBy, bson.DocElem{Name: k, Value: 1})
		}
	}
	return q
}

func (q *Query) SetMaxScan(n int) *Query {
	q.maxScan = n
	return q
}

func (q *Query) SetMaxTime(duration time.Duration) *Query {
	q.maxDuration = int(duration / time.Millisecond)
	return q
}

func (q *Query) Skip(n int) *Query {
	q.skip = n
	return q
}

func (q *Query) Limit(n int) *Query {
	q.limit = n
	return q
}

func (q *Query) Apply(change Change, result interface{}) (*ChangeInfo, error) {
	var mid = M.Start("Query-Apply")
	defer M.Done(mid)
	if Mock && chk_mock("Query-Apply") {
		return nil, MockError
	}
	info, err := q.collection.FindAndModify(q.query, q.orderBy, change.Update, q.selector, change.Upsert, change.ReturnNew, result)
	if info.Matched == 0 && !change.Upsert {
		return nil, ErrNotFound
	}
	return NewChangeInfo(info), err
}

//func (q *Query) Hint(indexKey ...string) *Query {
//	return q
//}

func (q *Query) Explain(result interface{}) error {
	return nil
}

func (q *Query) Hint(hint string) *Query {
	q.hint = hint
	return q
}

type Pipe struct {
	collection *mongoc.Collection
	pipeLine   interface{}
	batchSize  int
	allowDisk  bool
}

func (p *Pipe) Batch(n int) *Pipe {
	p.batchSize = n
	return p
}

func (p *Pipe) One(result interface{}) error {
	var mid = M.Start("Pipe-One")
	defer M.Done(mid)
	if Mock && chk_mock("Pipe-One") {
		return MockError
	}
	return p.collection.Pipe(p.pipeLine, result)
}

func (p *Pipe) All(result interface{}) error {
	var mid = M.Start("Pipe-All")
	defer M.Done(mid)
	if Mock && chk_mock("Pipe-All") {
		return MockError
	}
	return p.collection.Pipe(p.pipeLine, result)
}

func (p *Pipe) AllowDiskUse() *Pipe {
	p.allowDisk = true
	return p
}

func (p *Pipe) Explain(result interface{}) error {
	return nil
}

type BulkResult *mongoc.BulkReply

func NewBulkResult(reply *mongoc.BulkReply) BulkResult {
	if reply == nil {
		return nil
	}
	return BulkResult(reply)
}

type Bulk struct {
	collection *mongoc.Collection
	bulk       *mongoc.Bulk
	//opcount    int
}

func (b *Bulk) Unordered() {
	b.bulk.Ordered = false
}

func (b *Bulk) Insert(docs ...interface{}) {
	b.bulk.Insert(docs...)
}

func (b *Bulk) Remove(selectors ...interface{}) {
	for _, sel := range selectors {
		b.bulk.Remove(sel)
	}
}

func (b *Bulk) RemoveAll(selectors ...interface{}) {
	for _, sel := range selectors {
		b.bulk.Remove(sel)
	}
}

func (b *Bulk) Update(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.Update requires an even number of parameters")
	}

	for i := 0; i < len(pairs); i += 2 {
		b.bulk.Update(pairs[i], pairs[i+1], false)
	}
}

func (b *Bulk) UpdateAll(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.UpdateAll requires an even number of parameters")
	}

	for i := 0; i < len(pairs); i += 2 {
		b.bulk.Update(pairs[i], pairs[i+1], false)
	}
}

func (b *Bulk) Upsert(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.Upsert requires an even number of parameters")
	}

	for i := 0; i < len(pairs); i += 2 {
		b.bulk.Update(pairs[i], pairs[i+1], true)
	}
}

func (b *Bulk) Run() (BulkResult, error) {
	var mid = M.Start("Bulk-Run")
	defer M.Done(mid)
	if Mock && chk_mock("Bulk-Run") {
		return nil, MockError
	}
	reply, err := b.bulk.Execute()
	return NewBulkResult(reply), err
}
