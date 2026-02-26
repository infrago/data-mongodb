package data_mongodb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/bamgoo/base"
	"github.com/bamgoo/data"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	mongodbDriver struct{}

	mongodbConnection struct {
		instance *data.Instance
		client   *mongo.Client
		db       *mongo.Database
	}

	mongoBase struct {
		inst  *data.Instance
		conn  *mongodbConnection
		mutex sync.RWMutex
		err   error
		txCtx mongo.SessionContext
		txSes mongo.Session
	}

	mongoTable struct {
		base   *mongoBase
		name   string
		source string
		key    string
		fields Vars
	}

	mongoView struct {
		base   *mongoBase
		name   string
		source string
		key    string
		fields Vars
	}

	mongoModel struct {
		mongoView
	}
)

func (d *mongodbDriver) Connect(inst *data.Instance) (data.Connection, error) {
	return &mongodbConnection{instance: inst}, nil
}

func (c *mongodbConnection) Open() error {
	dsn := strings.TrimSpace(c.instance.Config.Url)
	if dsn == "" {
		if v, ok := c.instance.Setting["dsn"].(string); ok {
			dsn = strings.TrimSpace(v)
		}
	}
	if dsn == "" {
		dsn = "mongodb://127.0.0.1:27017"
	}

	dbName := strings.TrimSpace(c.instance.Config.Schema)
	if dbName == "" {
		if v, ok := c.instance.Setting["database"].(string); ok {
			dbName = strings.TrimSpace(v)
		}
	}
	if dbName == "" {
		dbName = "bamgoo"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(dsn))
	if err != nil {
		return err
	}
	if err := cli.Ping(ctx, nil); err != nil {
		_ = cli.Disconnect(ctx)
		return err
	}
	c.client = cli
	c.db = cli.Database(dbName)
	return nil
}

func (c *mongodbConnection) Close() error {
	if c.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.client.Disconnect(ctx)
	c.client = nil
	c.db = nil
	return err
}

func (c *mongodbConnection) Health() data.Health {
	return data.Health{Workload: 0}
}

func (c *mongodbConnection) DB() *sql.DB { return nil }

func (c *mongodbConnection) Dialect() data.Dialect { return mongoDialect{} }

func (c *mongodbConnection) Base(inst *data.Instance) data.DataBase {
	return &mongoBase{inst: inst, conn: c}
}

type mongoDialect struct{}

func (mongoDialect) Name() string             { return "mongodb" }
func (mongoDialect) Quote(s string) string    { return s }
func (mongoDialect) Placeholder(_ int) string { return "?" }
func (mongoDialect) SupportsILike() bool      { return true }
func (mongoDialect) SupportsReturning() bool  { return true }
func (b *mongoBase) Capabilities() data.Capabilities {
	return data.Capabilities{Dialect: "mongodb", ILike: true, Returning: true, Join: true, Group: true, Having: true, Aggregate: true, KeysetAfter: true, JsonContains: true, ArrayOverlap: true, JsonElemMatch: true}
}
func (b *mongoBase) Begin() error {
	b.mutex.RLock()
	active := b.txCtx != nil
	b.mutex.RUnlock()
	if active {
		return nil
	}
	ses, err := b.conn.client.StartSession()
	if err != nil {
		b.setError(err)
		return err
	}
	sc := mongo.NewSessionContext(context.Background(), ses)
	if err := sc.StartTransaction(); err != nil {
		ses.EndSession(context.Background())
		b.setError(err)
		return err
	}
	b.mutex.Lock()
	b.txSes = ses
	b.txCtx = sc
	b.mutex.Unlock()
	b.setError(nil)
	return nil
}

func (b *mongoBase) Commit() error {
	b.mutex.RLock()
	sc := b.txCtx
	ses := b.txSes
	b.mutex.RUnlock()
	if sc == nil || ses == nil {
		return nil
	}
	err := sc.CommitTransaction(sc)
	ses.EndSession(context.Background())
	b.mutex.Lock()
	b.txCtx = nil
	b.txSes = nil
	b.mutex.Unlock()
	b.setError(err)
	return err
}

func (b *mongoBase) Rollback() error {
	b.mutex.RLock()
	sc := b.txCtx
	ses := b.txSes
	b.mutex.RUnlock()
	if sc == nil || ses == nil {
		return nil
	}
	err := sc.AbortTransaction(sc)
	ses.EndSession(context.Background())
	b.mutex.Lock()
	b.txCtx = nil
	b.txSes = nil
	b.mutex.Unlock()
	b.setError(err)
	return err
}

func (b *mongoBase) Close() error {
	return b.Rollback()
}
func (b *mongoBase) Tx(fn data.TxFunc) error {
	if fn == nil {
		return nil
	}
	b.mutex.RLock()
	hasTx := b.txCtx != nil
	b.mutex.RUnlock()
	if hasTx {
		if err := fn(b); err != nil {
			b.setError(err)
			return err
		}
		return b.Error()
	}
	ses, err := b.conn.client.StartSession()
	if err != nil {
		b.setError(err)
		return err
	}
	defer ses.EndSession(context.Background())
	_, err = ses.WithTransaction(context.Background(), func(sc mongo.SessionContext) (interface{}, error) {
		b.mutex.Lock()
		b.txSes = ses
		b.txCtx = sc
		b.mutex.Unlock()
		b.setError(nil)
		if err := fn(b); err != nil {
			return nil, err
		}
		if b.Error() != nil {
			return nil, b.Error()
		}
		return nil, nil
	})
	b.mutex.Lock()
	b.txSes = nil
	b.txCtx = nil
	b.mutex.Unlock()
	b.setError(err)
	return err
}
func (b *mongoBase) setError(err error) { b.mutex.Lock(); b.err = err; b.mutex.Unlock() }
func (b *mongoBase) Error() error       { b.mutex.RLock(); defer b.mutex.RUnlock(); return b.err }
func (b *mongoBase) ClearError()        { b.setError(nil) }
func (b *mongoBase) opContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	b.mutex.RLock()
	sc := b.txCtx
	b.mutex.RUnlock()
	base := context.Background()
	if sc != nil {
		base = sc
	}
	if timeout > 0 {
		return context.WithTimeout(base, timeout)
	}
	return base, func() {}
}
func (b *mongoBase) Parse(args ...Any) (string, []Any) {
	q, err := data.Parse(args...)
	if err != nil {
		b.setError(err)
		return "", nil
	}
	f, err := q.Filter, error(nil)
	m := bson.M{}
	if f != nil {
		m, err = exprToFilter(f)
	}
	if err != nil {
		b.setError(err)
		return "", nil
	}
	raw, _ := json.Marshal(m)
	b.setError(nil)
	return string(raw), nil
}
func (b *mongoBase) Raw(query string, args ...Any) []Map {
	cmd := strings.TrimSpace(query)
	lower := strings.ToLower(cmd)
	switch {
	case strings.HasPrefix(lower, "aggregate "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			b.setError(fmt.Errorf("raw aggregate requires collection name"))
			return nil
		}
		coll := parts[1]
		pipeline, err := parsePipelineArg(firstArg(args))
		if err != nil {
			b.setError(err)
			return nil
		}
		ctx, cancel := b.opContext(20 * time.Second)
		defer cancel()
		cur, err := b.conn.db.Collection(coll).Aggregate(ctx, pipeline)
		if err != nil {
			b.setError(err)
			return nil
		}
		defer cur.Close(ctx)
		out := make([]Map, 0)
		for cur.Next(ctx) {
			row := bson.M{}
			if err := cur.Decode(&row); err != nil {
				b.setError(err)
				return nil
			}
			out = append(out, bsonToMap(row))
		}
		if err := cur.Err(); err != nil {
			b.setError(err)
			return nil
		}
		b.setError(nil)
		return out
	case strings.HasPrefix(lower, "find "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			b.setError(fmt.Errorf("raw find requires collection name"))
			return nil
		}
		coll := parts[1]
		filter := bson.M{}
		if len(args) > 0 {
			f, err := toBsonMap(args[0])
			if err != nil {
				b.setError(err)
				return nil
			}
			filter = f
		}
		opts := options.Find()
		if len(args) > 1 {
			if m, ok := args[1].(Map); ok {
				if sorts, ok := m["sort"].(Map); ok {
					sd := bson.D{}
					for k, v := range sorts {
						dir := int32(1)
						switch vv := v.(type) {
						case int:
							if vv < 0 {
								dir = -1
							}
						case int64:
							if vv < 0 {
								dir = -1
							}
						}
						sd = append(sd, bson.E{Key: k, Value: dir})
					}
					opts.SetSort(sd)
				}
				if lim, ok := parseInt64(m["limit"]); ok && lim > 0 {
					opts.SetLimit(lim)
				}
				if off, ok := parseInt64(m["offset"]); ok && off > 0 {
					opts.SetSkip(off)
				}
			}
		}
		ctx, cancel := b.opContext(20 * time.Second)
		defer cancel()
		cur, err := b.conn.db.Collection(coll).Find(ctx, filter, opts)
		if err != nil {
			b.setError(err)
			return nil
		}
		defer cur.Close(ctx)
		out := make([]Map, 0)
		for cur.Next(ctx) {
			row := bson.M{}
			if err := cur.Decode(&row); err != nil {
				b.setError(err)
				return nil
			}
			out = append(out, bsonToMap(row))
		}
		if err := cur.Err(); err != nil {
			b.setError(err)
			return nil
		}
		b.setError(nil)
		return out
	default:
		command, err := parseCommand(query, firstArg(args))
		if err != nil {
			b.setError(err)
			return nil
		}
		ctx, cancel := b.opContext(20 * time.Second)
		defer cancel()
		res := b.conn.db.RunCommand(ctx, command)
		if res.Err() != nil {
			b.setError(res.Err())
			return nil
		}
		row := bson.M{}
		if err := res.Decode(&row); err != nil {
			b.setError(err)
			return nil
		}
		b.setError(nil)
		return []Map{bsonToMap(row)}
	}
}
func (b *mongoBase) Exec(query string, args ...Any) int64 {
	cmd := strings.TrimSpace(query)
	lower := strings.ToLower(cmd)
	ctx, cancel := b.opContext(20 * time.Second)
	defer cancel()
	switch {
	case strings.HasPrefix(lower, "createcollection "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			b.setError(fmt.Errorf("createCollection requires collection name"))
			return 0
		}
		if err := b.conn.db.CreateCollection(ctx, parts[1]); err != nil {
			b.setError(err)
			return 0
		}
		b.setError(nil)
		return 1
	case strings.HasPrefix(lower, "dropcollection "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			b.setError(fmt.Errorf("dropCollection requires collection name"))
			return 0
		}
		if err := b.conn.db.Collection(parts[1]).Drop(ctx); err != nil {
			b.setError(err)
			return 0
		}
		b.setError(nil)
		return 1
	case strings.HasPrefix(lower, "deletemany "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			b.setError(fmt.Errorf("deleteMany requires collection name"))
			return 0
		}
		filter, err := toBsonMap(firstArg(args))
		if err != nil {
			b.setError(err)
			return 0
		}
		res, err := b.conn.db.Collection(parts[1]).DeleteMany(ctx, filter)
		if err != nil {
			b.setError(err)
			return 0
		}
		b.setError(nil)
		return res.DeletedCount
	case strings.HasPrefix(lower, "updatemany "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			b.setError(fmt.Errorf("updateMany requires collection name"))
			return 0
		}
		filter, err := toBsonMap(firstArg(args))
		if err != nil {
			b.setError(err)
			return 0
		}
		var update bson.M
		if len(args) > 1 {
			if m, ok := args[1].(Map); ok {
				update = buildUpdateDoc(m)
			} else {
				update, err = toBsonMap(args[1])
				if err != nil {
					b.setError(err)
					return 0
				}
			}
		} else {
			b.setError(fmt.Errorf("updateMany requires update doc"))
			return 0
		}
		res, err := b.conn.db.Collection(parts[1]).UpdateMany(ctx, filter, update)
		if err != nil {
			b.setError(err)
			return 0
		}
		b.setError(nil)
		return res.ModifiedCount
	case strings.HasPrefix(lower, "insertmany "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			b.setError(fmt.Errorf("insertMany requires collection name"))
			return 0
		}
		rows, err := toMapSlice(firstArg(args))
		if err != nil {
			b.setError(err)
			return 0
		}
		docs := make([]any, 0, len(rows))
		for _, row := range rows {
			docs = append(docs, bson.M(row))
		}
		res, err := b.conn.db.Collection(parts[1]).InsertMany(ctx, docs)
		if err != nil {
			b.setError(err)
			return 0
		}
		b.setError(nil)
		return int64(len(res.InsertedIDs))
	default:
		command, err := parseCommand(query, firstArg(args))
		if err != nil {
			b.setError(err)
			return 0
		}
		if err := b.conn.db.RunCommand(ctx, command).Err(); err != nil {
			b.setError(err)
			return 0
		}
		b.setError(nil)
		return 1
	}
}

func (b *mongoBase) Migrate(names ...string) {
	targets := names
	if len(targets) == 0 {
		for name := range data.Tables() {
			targets = append(targets, name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, name := range targets {
		t, ok := resolveTable(b.inst.Name, name)
		if !ok {
			continue
		}
		source := pickName(name, t.Table)
		if err := b.ensureCollection(ctx, source); err != nil {
			b.setError(err)
			return
		}
		if err := b.ensureIndexes(ctx, source, t); err != nil {
			b.setError(err)
			return
		}
	}
	b.setError(nil)
}

func (b *mongoBase) ensureCollection(ctx context.Context, name string) error {
	cols, err := b.conn.db.ListCollectionNames(ctx, bson.M{"name": name})
	if err != nil {
		return err
	}
	if len(cols) > 0 {
		return nil
	}
	return b.conn.db.CreateCollection(ctx, name)
}

func (b *mongoBase) ensureIndexes(ctx context.Context, source string, table data.Table) error {
	items := make([]mongo.IndexModel, 0)
	for i, idx := range table.Indexes {
		if len(idx.Fields) == 0 {
			continue
		}
		keys := bson.D{}
		for _, f := range idx.Fields {
			keys = append(keys, bson.E{Key: f, Value: 1})
		}
		name := strings.TrimSpace(idx.Name)
		if name == "" {
			name = fmt.Sprintf("idx_%s_%d", source, i+1)
		}
		items = append(items, mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name).SetUnique(idx.Unique)})
	}
	if table.Setting != nil {
		if raw, ok := table.Setting["indexes"].([]Map); ok {
			for i, idx := range raw {
				fields := parseStringList(idx["fields"])
				if len(fields) == 0 {
					continue
				}
				keys := bson.D{}
				for _, f := range fields {
					keys = append(keys, bson.E{Key: f, Value: 1})
				}
				name, _ := idx["name"].(string)
				if strings.TrimSpace(name) == "" {
					name = fmt.Sprintf("idx_%s_legacy_%d", source, i+1)
				}
				unique, _ := parseBool(idx["unique"])
				items = append(items, mongo.IndexModel{Keys: keys, Options: options.Index().SetName(name).SetUnique(unique)})
			}
		}
	}
	if len(items) == 0 {
		return nil
	}
	_, err := b.conn.db.Collection(source).Indexes().CreateMany(ctx, items)
	return err
}

func (b *mongoBase) Table(name string) data.DataTable {
	t, ok := resolveTable(b.inst.Name, name)
	if !ok {
		b.setError(fmt.Errorf("data table not found: %s", name))
		return &mongoTable{base: b, name: name, source: name, key: "id"}
	}
	return &mongoTable{base: b, name: name, source: pickName(name, t.Table), key: pickKey(t.Key), fields: t.Fields}
}

func (b *mongoBase) View(name string) data.DataView {
	v, ok := resolveView(b.inst.Name, name)
	if !ok {
		b.setError(fmt.Errorf("data view not found: %s", name))
		return &mongoView{base: b, name: name, source: name, key: "id"}
	}
	return &mongoView{base: b, name: name, source: pickName(name, v.View), key: pickKey(v.Key), fields: v.Fields}
}

func (b *mongoBase) Model(name string) data.DataModel {
	m, ok := resolveModel(b.inst.Name, name)
	if !ok {
		b.setError(fmt.Errorf("data model not found: %s", name))
		return &mongoModel{mongoView{base: b, name: name, source: name, key: "id"}}
	}
	return &mongoModel{mongoView{base: b, name: name, source: pickName(name, m.Model), key: pickKey(m.Key), fields: m.Fields}}
}

func (t *mongoTable) coll() *mongo.Collection { return t.base.conn.db.Collection(t.source) }

func (t *mongoTable) Create(dataIn Map) Map {
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	doc := bson.M(dataIn)
	res, err := t.coll().InsertOne(ctx, doc)
	if err != nil {
		t.base.setError(err)
		return nil
	}
	out := cloneMap(dataIn)
	if _, ok := out[t.key]; !ok {
		out[t.key] = res.InsertedID
	}
	t.base.setError(nil)
	return out
}

func (t *mongoTable) CreateMany(items []Map) []Map {
	if len(items) == 0 {
		t.base.setError(nil)
		return []Map{}
	}
	ctx, cancel := t.base.opContext(15 * time.Second)
	defer cancel()
	docs := make([]any, 0, len(items))
	for _, item := range items {
		docs = append(docs, bson.M(item))
	}
	res, err := t.coll().InsertMany(ctx, docs)
	if err != nil {
		t.base.setError(err)
		return nil
	}
	out := make([]Map, 0, len(items))
	for i, item := range items {
		m := cloneMap(item)
		if _, ok := m[t.key]; !ok && i < len(res.InsertedIDs) {
			m[t.key] = res.InsertedIDs[i]
		}
		out = append(out, m)
	}
	t.base.setError(nil)
	return out
}

func (t *mongoTable) Upsert(dataIn Map, args ...Any) Map {
	filter := make(Map)
	if len(args) > 0 {
		if m, ok := args[0].(Map); ok {
			for k, v := range m {
				filter[k] = v
			}
		}
	}
	if len(filter) == 0 {
		if id, ok := dataIn[t.key]; ok {
			filter[t.key] = id
		}
	}
	if len(filter) == 0 {
		return t.Create(dataIn)
	}
	upd := buildUpdateDoc(dataIn)
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	_, err := t.coll().UpdateOne(ctx, bson.M(filter), upd, options.Update().SetUpsert(true))
	if err != nil {
		t.base.setError(err)
		return nil
	}
	out := t.First(filter)
	return out
}

func (t *mongoTable) UpsertMany(items []Map, args ...Any) []Map {
	out := make([]Map, 0, len(items))
	for _, item := range items {
		one := t.Upsert(item, args...)
		if t.base.Error() != nil {
			return nil
		}
		out = append(out, one)
	}
	t.base.setError(nil)
	return out
}

func (t *mongoTable) Change(item Map, dataIn Map) Map {
	if item == nil || item[t.key] == nil {
		t.base.setError(fmt.Errorf("missing primary key %s", t.key))
		return nil
	}
	upd := buildUpdateDoc(dataIn)
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	_, err := t.coll().UpdateOne(ctx, bson.M{t.key: item[t.key]}, upd)
	if err != nil {
		t.base.setError(err)
		return nil
	}
	return t.First(Map{t.key: item[t.key]})
}

func (t *mongoTable) Remove(args ...Any) Map {
	item := t.First(args...)
	if t.base.Error() != nil || item == nil {
		return nil
	}
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	_, err := t.coll().DeleteOne(ctx, bson.M{t.key: item[t.key]})
	if err != nil {
		t.base.setError(err)
		return nil
	}
	t.base.setError(nil)
	return item
}

func (t *mongoTable) Update(sets Map, args ...Any) int64 {
	q, err := data.Parse(args...)
	if err != nil {
		t.base.setError(err)
		return 0
	}
	filter, err := exprToFilter(q.Filter)
	if err != nil {
		t.base.setError(err)
		return 0
	}
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	res, err := t.coll().UpdateMany(ctx, filter, buildUpdateDoc(sets))
	if err != nil {
		t.base.setError(err)
		return 0
	}
	t.base.setError(nil)
	return res.ModifiedCount
}

func (t *mongoTable) Delete(args ...Any) int64 {
	q, err := data.Parse(args...)
	if err != nil {
		t.base.setError(err)
		return 0
	}
	filter, err := exprToFilter(q.Filter)
	if err != nil {
		t.base.setError(err)
		return 0
	}
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	res, err := t.coll().DeleteMany(ctx, filter)
	if err != nil {
		t.base.setError(err)
		return 0
	}
	t.base.setError(nil)
	return res.DeletedCount
}

func (t *mongoTable) Entity(id Any) Map           { return t.First(Map{t.key: id}) }
func (t *mongoTable) Count(args ...Any) int64     { return (*mongoView)(t).Count(args...) }
func (t *mongoTable) Aggregate(args ...Any) []Map { return (*mongoView)(t).Aggregate(args...) }
func (t *mongoTable) First(args ...Any) Map       { return (*mongoView)(t).First(args...) }
func (t *mongoTable) Query(args ...Any) []Map     { return (*mongoView)(t).Query(args...) }
func (t *mongoTable) Range(next data.RangeFunc, args ...Any) Res {
	return (*mongoView)(t).Range(next, args...)
}
func (t *mongoTable) LimitRange(limit int64, next data.RangeFunc, args ...Any) Res {
	return (*mongoView)(t).LimitRange(limit, next, args...)
}
func (t *mongoTable) Limit(offset, limit int64, args ...Any) (int64, []Map) {
	return (*mongoView)(t).Limit(offset, limit, args...)
}
func (t *mongoTable) Page(offset, limit int64, args ...Any) data.PageResult {
	return (*mongoView)(t).Page(offset, limit, args...)
}
func (t *mongoTable) Group(field string, args ...Any) []Map {
	return (*mongoView)(t).Group(field, args...)
}

func (v *mongoView) coll() *mongo.Collection { return v.base.conn.db.Collection(v.source) }

func (v *mongoView) Count(args ...Any) int64 {
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return 0
	}
	filter, err := exprToFilter(q.Filter)
	if err != nil {
		v.base.setError(err)
		return 0
	}
	ctx, cancel := v.base.opContext(10 * time.Second)
	defer cancel()
	total, err := v.coll().CountDocuments(ctx, filter)
	if err != nil {
		v.base.setError(err)
		return 0
	}
	v.base.setError(nil)
	return total
}

func (v *mongoView) First(args ...Any) Map {
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return nil
	}
	q.Limit = 1
	items, err := v.queryWithQuery(q)
	if err != nil {
		v.base.setError(err)
		return nil
	}
	if len(items) == 0 {
		v.base.setError(nil)
		return nil
	}
	v.base.setError(nil)
	return items[0]
}

func (v *mongoView) Query(args ...Any) []Map {
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return nil
	}
	items, err := v.queryWithQuery(q)
	v.base.setError(err)
	return items
}

func (v *mongoView) Aggregate(args ...Any) []Map {
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return nil
	}
	if len(q.Aggs) == 0 {
		q.Aggs = []data.Agg{{Alias: "$count", Op: "count", Field: "*"}}
	}
	items, err := v.aggregateWithQuery(q)
	v.base.setError(err)
	return items
}

func (v *mongoView) Range(next data.RangeFunc, args ...Any) Res {
	return v.LimitRange(0, next, args...)
}

func (v *mongoView) LimitRange(limit int64, next data.RangeFunc, args ...Any) Res {
	if next == nil {
		return nil
	}
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return nil
	}
	if limit > 0 {
		q.Limit = limit
	}
	items, err := v.queryWithQuery(q)
	if err != nil {
		v.base.setError(err)
		return nil
	}
	for _, item := range items {
		if res := next(item); res != nil && res.Fail() {
			return res
		}
	}
	v.base.setError(nil)
	return nil
}

func (v *mongoView) Limit(offset, limit int64, args ...Any) (int64, []Map) {
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return 0, nil
	}
	q.Offset = offset
	q.Limit = limit
	total := int64(-1)
	if q.WithCount {
		total = v.Count(args...)
		if v.base.Error() != nil {
			return 0, nil
		}
	}
	items, err := v.queryWithQuery(q)
	if err != nil {
		v.base.setError(err)
		return 0, nil
	}
	v.base.setError(nil)
	return total, items
}

func (v *mongoView) Page(offset, limit int64, args ...Any) data.PageResult {
	total, items := v.Limit(offset, limit, args...)
	return data.PageResult{Offset: offset, Limit: limit, Total: total, Items: items}
}

func (v *mongoView) Group(field string, args ...Any) []Map {
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return nil
	}
	if len(q.Group) == 0 {
		q.Group = []string{field}
	}
	if len(q.Aggs) == 0 {
		q.Aggs = []data.Agg{{Alias: "$count", Op: "count", Field: "*"}}
	}
	items, err := v.aggregateWithQuery(q)
	v.base.setError(err)
	return items
}

func (m *mongoModel) First(args ...Any) Map   { return m.mongoView.First(args...) }
func (m *mongoModel) Query(args ...Any) []Map { return m.mongoView.Query(args...) }
func (m *mongoModel) Range(next data.RangeFunc, args ...Any) Res {
	return m.mongoView.Range(next, args...)
}
func (m *mongoModel) LimitRange(limit int64, next data.RangeFunc, args ...Any) Res {
	return m.mongoView.LimitRange(limit, next, args...)
}
func (m *mongoModel) Limit(offset, limit int64, args ...Any) (int64, []Map) {
	return m.mongoView.Limit(offset, limit, args...)
}
func (m *mongoModel) Page(offset, limit int64, args ...Any) data.PageResult {
	return m.mongoView.Page(offset, limit, args...)
}

func (v *mongoView) queryWithQuery(q data.Query) ([]Map, error) {
	if len(q.Aggs) > 0 || len(q.Group) > 0 {
		return v.aggregateWithQuery(q)
	}
	q = applyAfter(q)
	filter, err := exprToFilter(q.Filter)
	if err != nil {
		return nil, err
	}
	findOpts := options.Find()
	if len(q.Select) > 0 {
		proj := bson.M{}
		for _, field := range q.Select {
			proj[field] = 1
		}
		findOpts.SetProjection(proj)
	}
	if len(q.Sort) > 0 {
		s := bson.D{}
		for _, one := range q.Sort {
			d := 1
			if one.Desc {
				d = -1
			}
			s = append(s, bson.E{Key: one.Field, Value: d})
		}
		findOpts.SetSort(s)
	}
	if q.Offset > 0 {
		findOpts.SetSkip(q.Offset)
	}
	if q.Limit > 0 {
		findOpts.SetLimit(q.Limit)
	}
	ctx, cancel := v.base.opContext(15 * time.Second)
	defer cancel()
	cur, err := v.coll().Find(ctx, filter, findOpts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]Map, 0)
	for cur.Next(ctx) {
		m := bson.M{}
		if err := cur.Decode(&m); err != nil {
			return nil, err
		}
		out = append(out, bsonToMap(m))
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (v *mongoView) aggregateWithQuery(q data.Query) ([]Map, error) {
	q = applyAfter(q)
	pipeline := mongo.Pipeline{}
	filter, err := exprToFilter(q.Filter)
	if err != nil {
		return nil, err
	}
	if len(filter) > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}})
	}
	for _, join := range q.Joins {
		alias := strings.TrimSpace(join.Alias)
		if alias == "" {
			alias = strings.TrimSpace(join.From)
		}
		if strings.TrimSpace(join.LocalField) != "" && strings.TrimSpace(join.ForeignField) != "" {
			localAliases := []string{v.name, v.source}
			foreignAliases := []string{alias, join.From}
			pipeline = append(pipeline, bson.D{{Key: "$lookup", Value: bson.M{
				"from":         join.From,
				"localField":   normalizeMongoPathAliases(join.LocalField, localAliases),
				"foreignField": normalizeMongoPathAliases(join.ForeignField, foreignAliases),
				"as":           alias,
			}}})
			continue
		}
		if join.On != nil {
			letVars := bson.M{}
			localAliases := []string{v.name, v.source}
			foreignAliases := []string{alias, join.From}
			expr, err := exprToLookupExpr(join.On, localAliases, foreignAliases, letVars)
			if err != nil {
				return nil, err
			}
			lk := bson.M{
				"from":     join.From,
				"as":       alias,
				"pipeline": []bson.M{{"$match": bson.M{"$expr": expr}}},
			}
			if len(letVars) > 0 {
				lk["let"] = letVars
			}
			pipeline = append(pipeline, bson.D{{Key: "$lookup", Value: lk}})
			continue
		}
		return nil, fmt.Errorf("mongodb join requires localField/foreignField or on")
	}
	if len(q.Group) > 0 || len(q.Aggs) > 0 {
		groupID := bson.M{}
		for _, g := range q.Group {
			groupID[g] = "$" + g
		}
		if len(groupID) == 0 {
			groupID = nil
		}
		groupDoc := bson.M{"_id": groupID}
		for _, agg := range q.Aggs {
			op := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(agg.Op)), "$")
			field := agg.Field
			if field == "" {
				field = "*"
			}
			var target Any = "$" + field
			if field == "*" {
				target = 1
			}
			switch op {
			case "count":
				groupDoc[agg.Alias] = bson.M{"$sum": 1}
			case "sum":
				groupDoc[agg.Alias] = bson.M{"$sum": target}
			case "avg":
				groupDoc[agg.Alias] = bson.M{"$avg": target}
			case "min":
				groupDoc[agg.Alias] = bson.M{"$min": target}
			case "max":
				groupDoc[agg.Alias] = bson.M{"$max": target}
			default:
				return nil, fmt.Errorf("unsupported agg op %s", agg.Op)
			}
		}
		pipeline = append(pipeline, bson.D{{Key: "$group", Value: groupDoc}})
		if q.Having != nil {
			having, err := exprToFilter(q.Having)
			if err != nil {
				return nil, err
			}
			if len(having) > 0 {
				pipeline = append(pipeline, bson.D{{Key: "$match", Value: having}})
			}
		}
	}
	if len(q.Sort) > 0 {
		s := bson.D{}
		for _, one := range q.Sort {
			d := 1
			if one.Desc {
				d = -1
			}
			s = append(s, bson.E{Key: one.Field, Value: d})
		}
		pipeline = append(pipeline, bson.D{{Key: "$sort", Value: s}})
	}
	if q.Offset > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$skip", Value: q.Offset}})
	}
	if q.Limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: q.Limit}})
	}

	ctx, cancel := v.base.opContext(15 * time.Second)
	defer cancel()
	cur, err := v.coll().Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]Map, 0)
	for cur.Next(ctx) {
		m := bson.M{}
		if err := cur.Decode(&m); err != nil {
			return nil, err
		}
		flat := bsonToMap(m)
		if id, ok := m["_id"].(bson.M); ok {
			for k, v := range id {
				flat[k] = normalizeBsonValue(v)
			}
		}
		out = append(out, flat)
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func exprToFilter(expr data.Expr) (bson.M, error) {
	switch e := expr.(type) {
	case nil, data.TrueExpr:
		return bson.M{}, nil
	case data.AndExpr:
		arr := make([]bson.M, 0, len(e.Items))
		for _, item := range e.Items {
			m, err := exprToFilter(item)
			if err != nil {
				return nil, err
			}
			if len(m) > 0 {
				arr = append(arr, m)
			}
		}
		if len(arr) == 0 {
			return bson.M{}, nil
		}
		return bson.M{"$and": arr}, nil
	case data.OrExpr:
		arr := make([]bson.M, 0, len(e.Items))
		for _, item := range e.Items {
			m, err := exprToFilter(item)
			if err != nil {
				return nil, err
			}
			if len(m) > 0 {
				arr = append(arr, m)
			}
		}
		if len(arr) == 0 {
			return bson.M{}, nil
		}
		return bson.M{"$or": arr}, nil
	case data.NotExpr:
		m, err := exprToFilter(e.Item)
		if err != nil {
			return nil, err
		}
		return bson.M{"$nor": []bson.M{m}}, nil
	case data.ExistsExpr:
		return bson.M{e.Field: bson.M{"$exists": e.Yes}}, nil
	case data.NullExpr:
		if e.Yes {
			return bson.M{e.Field: nil}, nil
		}
		return bson.M{e.Field: bson.M{"$ne": nil}}, nil
	case data.CmpExpr:
		return cmpToFilter(e)
	case data.RawExpr:
		return bson.M{}, nil
	default:
		return nil, fmt.Errorf("unsupported expression %T", e)
	}
}

func cmpToFilter(c data.CmpExpr) (bson.M, error) {
	field := c.Field
	value := c.Value
	switch c.Op {
	case data.OpEq:
		return bson.M{field: normalizeCmpValue(value)}, nil
	case data.OpNe:
		return bson.M{field: bson.M{"$ne": normalizeCmpValue(value)}}, nil
	case data.OpGt:
		return bson.M{field: bson.M{"$gt": normalizeCmpValue(value)}}, nil
	case data.OpGte:
		return bson.M{field: bson.M{"$gte": normalizeCmpValue(value)}}, nil
	case data.OpLt:
		return bson.M{field: bson.M{"$lt": normalizeCmpValue(value)}}, nil
	case data.OpLte:
		return bson.M{field: bson.M{"$lte": normalizeCmpValue(value)}}, nil
	case data.OpIn:
		return bson.M{field: bson.M{"$in": toAnySlice(value)}}, nil
	case data.OpNin:
		return bson.M{field: bson.M{"$nin": toAnySlice(value)}}, nil
	case data.OpLike:
		return bson.M{field: primitive.Regex{Pattern: likeToRegex(fmt.Sprintf("%v", value)), Options: ""}}, nil
	case data.OpILike:
		return bson.M{field: primitive.Regex{Pattern: likeToRegex(fmt.Sprintf("%v", value)), Options: "i"}}, nil
	case data.OpRegex:
		return bson.M{field: primitive.Regex{Pattern: fmt.Sprintf("%v", value), Options: "i"}}, nil
	case data.OpContains:
		s := toAnySlice(value)
		if len(s) > 0 {
			return bson.M{field: bson.M{"$all": s}}, nil
		}
		if m, ok := value.(Map); ok {
			return bson.M{field: bson.M{"$all": []any{m}}}, nil
		}
		return bson.M{field: value}, nil
	case data.OpOverlap:
		return bson.M{field: bson.M{"$in": toAnySlice(value)}}, nil
	case data.OpElemMatch:
		if m, ok := value.(Map); ok {
			return bson.M{field: bson.M{"$elemMatch": bson.M(m)}}, nil
		}
		return bson.M{field: bson.M{"$elemMatch": value}}, nil
	default:
		return nil, fmt.Errorf("unsupported compare operator %s", c.Op)
	}
}

func buildUpdateDoc(input Map) bson.M {
	setPart := bson.M{}
	incPart := bson.M{}
	unsetPart := bson.M{}
	pushPart := bson.M{}
	pullPart := bson.M{}
	addSetPart := bson.M{}

	for k, v := range input {
		switch k {
		case data.UpdSet:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					setPart[kk] = vv
				}
			}
		case data.UpdInc:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					incPart[kk] = vv
				}
			}
		case data.UpdUnset:
			switch vv := v.(type) {
			case string:
				unsetPart[vv] = ""
			case []string:
				for _, one := range vv {
					unsetPart[one] = ""
				}
			case []Any:
				for _, one := range vv {
					if s, ok := one.(string); ok {
						unsetPart[s] = ""
					}
				}
			case Map:
				for kk := range vv {
					unsetPart[kk] = ""
				}
			}
		case data.UpdPush:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					arr := toAnySlice(vv)
					if len(arr) > 1 {
						pushPart[kk] = bson.M{"$each": arr}
					} else if len(arr) == 1 {
						pushPart[kk] = arr[0]
					}
				}
			}
		case data.UpdPull:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					arr := toAnySlice(vv)
					if len(arr) > 1 {
						pullPart[kk] = bson.M{"$in": arr}
					} else if len(arr) == 1 {
						pullPart[kk] = arr[0]
					}
				}
			}
		case data.UpdAddToSet:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					arr := toAnySlice(vv)
					if len(arr) > 1 {
						addSetPart[kk] = bson.M{"$each": arr}
					} else if len(arr) == 1 {
						addSetPart[kk] = arr[0]
					}
				}
			}
		case data.UpdSetPath:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					setPart[kk] = vv
				}
			}
		case data.UpdUnsetPath:
			switch vv := v.(type) {
			case string:
				unsetPart[vv] = ""
			case []string:
				for _, one := range vv {
					unsetPart[one] = ""
				}
			case []Any:
				for _, one := range vv {
					if s, ok := one.(string); ok {
						unsetPart[s] = ""
					}
				}
			}
		default:
			if !strings.HasPrefix(k, "$") {
				setPart[k] = v
			}
		}
	}

	out := bson.M{}
	if len(setPart) > 0 {
		out["$set"] = setPart
	}
	if len(incPart) > 0 {
		out["$inc"] = incPart
	}
	if len(unsetPart) > 0 {
		out["$unset"] = unsetPart
	}
	if len(pushPart) > 0 {
		out["$push"] = pushPart
	}
	if len(pullPart) > 0 {
		out["$pull"] = pullPart
	}
	if len(addSetPart) > 0 {
		out["$addToSet"] = addSetPart
	}
	if len(out) == 0 {
		out["$set"] = bson.M{}
	}
	return out
}

func applyAfter(q data.Query) data.Query {
	if len(q.After) == 0 || len(q.Sort) == 0 {
		return q
	}
	sf := q.Sort[0]
	val, ok := q.After[sf.Field]
	if !ok {
		val, ok = q.After["$value"]
	}
	if !ok {
		return q
	}
	op := data.OpGt
	if sf.Desc {
		op = data.OpLt
	}
	afterExpr := data.CmpExpr{Field: sf.Field, Op: op, Value: val}
	if q.Filter == nil {
		q.Filter = afterExpr
		return q
	}
	if _, ok := q.Filter.(data.TrueExpr); ok {
		q.Filter = afterExpr
		return q
	}
	q.Filter = data.AndExpr{Items: []data.Expr{q.Filter, afterExpr}}
	return q
}

func normalizeMongoPathAliases(field string, aliases []string) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return field
	}
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		parts := strings.SplitN(field, ".", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == alias {
			return strings.TrimSpace(parts[1])
		}
	}
	return field
}

func exprToLookupExpr(expr data.Expr, localAliases []string, foreignAliases []string, letVars bson.M) (Any, error) {
	switch e := expr.(type) {
	case nil, data.TrueExpr:
		return bson.M{"$literal": true}, nil
	case data.AndExpr:
		items := make([]Any, 0, len(e.Items))
		for _, one := range e.Items {
			x, err := exprToLookupExpr(one, localAliases, foreignAliases, letVars)
			if err != nil {
				return nil, err
			}
			items = append(items, x)
		}
		return bson.M{"$and": items}, nil
	case data.OrExpr:
		items := make([]Any, 0, len(e.Items))
		for _, one := range e.Items {
			x, err := exprToLookupExpr(one, localAliases, foreignAliases, letVars)
			if err != nil {
				return nil, err
			}
			items = append(items, x)
		}
		return bson.M{"$or": items}, nil
	case data.NotExpr:
		x, err := exprToLookupExpr(e.Item, localAliases, foreignAliases, letVars)
		if err != nil {
			return nil, err
		}
		return bson.M{"$not": []Any{x}}, nil
	case data.CmpExpr:
		left, err := lookupOperandFromField(e.Field, true, localAliases, foreignAliases, letVars)
		if err != nil {
			return nil, err
		}
		right, err := lookupOperandFromValue(e.Value, localAliases, foreignAliases, letVars)
		if err != nil {
			return nil, err
		}
		switch e.Op {
		case data.OpEq:
			return bson.M{"$eq": []Any{left, right}}, nil
		case data.OpNe:
			return bson.M{"$ne": []Any{left, right}}, nil
		case data.OpGt:
			return bson.M{"$gt": []Any{left, right}}, nil
		case data.OpGte:
			return bson.M{"$gte": []Any{left, right}}, nil
		case data.OpLt:
			return bson.M{"$lt": []Any{left, right}}, nil
		case data.OpLte:
			return bson.M{"$lte": []Any{left, right}}, nil
		default:
			return nil, fmt.Errorf("unsupported join on operator %s", e.Op)
		}
	case data.ExistsExpr:
		f, err := lookupOperandFromField(e.Field, false, localAliases, foreignAliases, letVars)
		if err != nil {
			return nil, err
		}
		if e.Yes {
			return bson.M{"$ne": []Any{f, nil}}, nil
		}
		return bson.M{"$eq": []Any{f, nil}}, nil
	case data.NullExpr:
		f, err := lookupOperandFromField(e.Field, false, localAliases, foreignAliases, letVars)
		if err != nil {
			return nil, err
		}
		if e.Yes {
			return bson.M{"$eq": []Any{f, nil}}, nil
		}
		return bson.M{"$ne": []Any{f, nil}}, nil
	default:
		return nil, fmt.Errorf("unsupported join on expr %T", e)
	}
}

func lookupOperandFromValue(v Any, localAliases []string, foreignAliases []string, letVars bson.M) (Any, error) {
	switch vv := v.(type) {
	case data.FieldRef:
		return lookupOperandFromField(string(vv), false, localAliases, foreignAliases, letVars)
	default:
		return vv, nil
	}
}

func lookupOperandFromField(field string, defaultLocal bool, localAliases []string, foreignAliases []string, letVars bson.M) (Any, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil, fmt.Errorf("empty field in join expression")
	}
	if matchPrefix(field, foreignAliases) {
		return "$" + stripPrefix(field), nil
	}
	if matchPrefix(field, localAliases) {
		key := "l_" + strings.ReplaceAll(stripPrefix(field), ".", "_")
		letVars[key] = "$" + stripPrefix(field)
		return "$$" + key, nil
	}
	if defaultLocal {
		key := "l_" + strings.ReplaceAll(field, ".", "_")
		letVars[key] = "$" + field
		return "$$" + key, nil
	}
	return "$" + field, nil
}

func matchPrefix(field string, aliases []string) bool {
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		if field == alias || strings.HasPrefix(field, alias+".") {
			return true
		}
	}
	return false
}

func stripPrefix(field string) string {
	parts := strings.SplitN(field, ".", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return field
}

func firstArg(args []Any) Any {
	if len(args) == 0 {
		return nil
	}
	return args[0]
}

func parsePipelineArg(v Any) (mongo.Pipeline, error) {
	if v == nil {
		return mongo.Pipeline{}, nil
	}
	switch vv := v.(type) {
	case []Map:
		out := make(mongo.Pipeline, 0, len(vv))
		for _, m := range vv {
			out = append(out, mapToDoc(m))
		}
		return out, nil
	case []Any:
		out := make(mongo.Pipeline, 0, len(vv))
		for _, item := range vv {
			m, err := toBsonMap(item)
			if err != nil {
				return nil, err
			}
			out = append(out, mapToDoc(Map(m)))
		}
		return out, nil
	case string:
		var arr []map[string]any
		if err := json.Unmarshal([]byte(vv), &arr); err != nil {
			return nil, err
		}
		out := make(mongo.Pipeline, 0, len(arr))
		for _, m := range arr {
			out = append(out, mapToDoc(Map(m)))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("invalid pipeline arg %T", v)
	}
}

func toBsonMap(v Any) (bson.M, error) {
	if v == nil {
		return bson.M{}, nil
	}
	switch vv := v.(type) {
	case bson.M:
		return vv, nil
	case Map:
		return bson.M(vv), nil
	case string:
		out := bson.M{}
		if err := bson.UnmarshalExtJSON([]byte(vv), false, &out); err == nil {
			return out, nil
		}
		if err := json.Unmarshal([]byte(vv), &out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to bson.M", v)
	}
}

func toMapSlice(v Any) ([]Map, error) {
	switch vv := v.(type) {
	case []Map:
		return vv, nil
	case []Any:
		out := make([]Map, 0, len(vv))
		for _, one := range vv {
			m, ok := one.(Map)
			if !ok {
				return nil, fmt.Errorf("invalid row type %T", one)
			}
			out = append(out, m)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("invalid rows type %T", v)
	}
}

func parseCommand(query string, arg Any) (bson.M, error) {
	cmd := strings.TrimSpace(query)
	if strings.EqualFold(cmd, "command") || cmd == "" {
		return toBsonMap(arg)
	}
	if strings.HasPrefix(strings.TrimSpace(cmd), "{") {
		return toBsonMap(cmd)
	}
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	return bson.M{parts[0]: 1}, nil
}

func mapToDoc(m Map) bson.D {
	out := bson.D{}
	for k, v := range m {
		out = append(out, bson.E{Key: k, Value: v})
	}
	return out
}

func parseInt64(v Any) (int64, bool) {
	switch vv := v.(type) {
	case int:
		return int64(vv), true
	case int64:
		return vv, true
	case float64:
		return int64(vv), true
	default:
		return 0, false
	}
}

func resolveTable(baseName, name string) (data.Table, bool) {
	all := data.Tables()
	for _, key := range []string{baseName + "." + name, "*." + name, name} {
		if v, ok := all[key]; ok {
			return v, true
		}
	}
	return data.Table{}, false
}

func resolveView(baseName, name string) (data.View, bool) {
	all := data.Views()
	for _, key := range []string{baseName + "." + name, "*." + name, name} {
		if v, ok := all[key]; ok {
			return v, true
		}
	}
	return data.View{}, false
}

func resolveModel(baseName, name string) (data.Model, bool) {
	all := data.Models()
	for _, key := range []string{baseName + "." + name, "*." + name, name} {
		if v, ok := all[key]; ok {
			return v, true
		}
	}
	return data.Model{}, false
}

func pickName(name, own string) string {
	if strings.TrimSpace(own) != "" {
		return own
	}
	return strings.ReplaceAll(name, ".", "_")
}

func pickKey(key string) string {
	if strings.TrimSpace(key) != "" {
		return key
	}
	return "id"
}

func parseStringList(val Any) []string {
	out := make([]string, 0)
	switch vv := val.(type) {
	case string:
		for _, one := range strings.Split(vv, ",") {
			one = strings.TrimSpace(one)
			if one != "" {
				out = append(out, one)
			}
		}
	case []string:
		for _, one := range vv {
			one = strings.TrimSpace(one)
			if one != "" {
				out = append(out, one)
			}
		}
	case []Any:
		for _, one := range vv {
			if s, ok := one.(string); ok {
				s = strings.TrimSpace(s)
				if s != "" {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

func parseBool(v Any) (bool, bool) {
	switch vv := v.(type) {
	case bool:
		return vv, true
	case string:
		vv = strings.TrimSpace(strings.ToLower(vv))
		if vv == "true" || vv == "1" || vv == "yes" {
			return true, true
		}
		if vv == "false" || vv == "0" || vv == "no" {
			return false, true
		}
	}
	return false, false
}

func cloneMap(in Map) Map {
	out := Map{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func bsonToMap(m bson.M) Map {
	out := Map{}
	for k, v := range m {
		out[k] = normalizeBsonValue(v)
	}
	return out
}

func normalizeBsonValue(v Any) Any {
	switch vv := v.(type) {
	case primitive.ObjectID:
		return vv.Hex()
	case bson.M:
		return bsonToMap(vv)
	case []any:
		arr := make([]Any, 0, len(vv))
		for _, one := range vv {
			arr = append(arr, normalizeBsonValue(one))
		}
		return arr
	default:
		return vv
	}
}

func toAnySlice(v Any) []Any {
	switch vv := v.(type) {
	case nil:
		return []Any{}
	case []Any:
		return vv
	case []string:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []int:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []int64:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []float64:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	default:
		return []Any{vv}
	}
}

func likeToRegex(v string) string {
	v = strings.ReplaceAll(v, ".", "\\.")
	v = strings.ReplaceAll(v, "%", ".*")
	v = strings.ReplaceAll(v, "_", ".")
	if !strings.HasPrefix(v, ".*") {
		v = "^" + v
	}
	if !strings.HasSuffix(v, ".*") {
		v = v + "$"
	}
	return v
}

func normalizeCmpValue(v Any) Any {
	switch vv := v.(type) {
	case data.FieldRef:
		// Mongo expr field refs require aggregation pipeline; fallback to raw string marker.
		return "$" + string(vv)
	default:
		return v
	}
}
