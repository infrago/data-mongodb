package data_mongodb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/data"
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
		mode  string
		ctx   context.Context
		tmo   time.Duration
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

type mongoCacheValue struct {
	expireAt int64
	items    []Map
	total    int64
}

var mongoCacheRegistry sync.Map
var mongoCacheCount sync.Map

func (b *mongoBase) fieldMappingEnabled() bool {
	return b != nil && b.inst != nil && b.inst.Config.Mapping
}

func (b *mongoBase) storageField(field string) string {
	field = strings.TrimSpace(field)
	if field == "" || !b.fieldMappingEnabled() {
		return field
	}
	parts := strings.Split(field, ".")
	for i := range parts {
		parts[i] = data.SnakeFieldPath(parts[i])
	}
	return strings.Join(parts, ".")
}

func (b *mongoBase) appField(field string) string {
	field = strings.TrimSpace(field)
	if field == "" || !b.fieldMappingEnabled() {
		return field
	}
	parts := strings.Split(field, ".")
	for i := range parts {
		parts[i] = data.CamelFieldPath(parts[i])
	}
	return strings.Join(parts, ".")
}

func (b *mongoBase) toStorageMap(input Map) Map {
	if input == nil || !b.fieldMappingEnabled() {
		return input
	}
	out := Map{}
	for k, v := range input {
		out[b.storageField(k)] = v
	}
	return out
}

func (b *mongoBase) toAppMap(input Map) Map {
	if input == nil || !b.fieldMappingEnabled() {
		return input
	}
	out := Map{}
	for k, v := range input {
		if strings.HasPrefix(k, "$") {
			out[k] = v
			continue
		}
		out[b.appField(k)] = v
	}
	return out
}

func (b *mongoBase) mapQueryToStorage(q data.Query) data.Query {
	if !b.fieldMappingEnabled() {
		return q
	}
	if len(q.Select) > 0 {
		out := make([]string, 0, len(q.Select))
		for _, one := range q.Select {
			out = append(out, b.storageField(one))
		}
		q.Select = out
	}
	if len(q.Sort) > 0 {
		out := make([]data.Sort, 0, len(q.Sort))
		for _, one := range q.Sort {
			out = append(out, data.Sort{Field: b.storageField(one.Field), Desc: one.Desc})
		}
		q.Sort = out
	}
	if len(q.Group) > 0 {
		out := make([]string, 0, len(q.Group))
		for _, one := range q.Group {
			out = append(out, b.storageField(one))
		}
		q.Group = out
	}
	if len(q.Aggs) > 0 {
		out := make([]data.Agg, 0, len(q.Aggs))
		for _, one := range q.Aggs {
			field := one.Field
			if field != "*" && field != "" {
				field = b.storageField(field)
			}
			out = append(out, data.Agg{Alias: one.Alias, Op: one.Op, Field: field})
		}
		q.Aggs = out
	}
	if len(q.After) > 0 {
		after := Map{}
		for k, v := range q.After {
			after[b.storageField(k)] = v
		}
		q.After = after
	}
	if len(q.Joins) > 0 {
		out := make([]data.Join, 0, len(q.Joins))
		for _, j := range q.Joins {
			out = append(out, data.Join{
				From:         j.From,
				Alias:        j.Alias,
				Type:         j.Type,
				LocalField:   b.storageField(j.LocalField),
				ForeignField: b.storageField(j.ForeignField),
				On:           b.mapExprToStorage(j.On),
			})
		}
		q.Joins = out
	}
	q.Filter = b.mapExprToStorage(q.Filter)
	q.Having = b.mapExprToStorage(q.Having)
	return q
}

func (b *mongoBase) mapExprToStorage(expr data.Expr) data.Expr {
	switch e := expr.(type) {
	case nil:
		return nil
	case data.TrueExpr:
		return e
	case data.AndExpr:
		items := make([]data.Expr, 0, len(e.Items))
		for _, one := range e.Items {
			items = append(items, b.mapExprToStorage(one))
		}
		return data.AndExpr{Items: items}
	case data.OrExpr:
		items := make([]data.Expr, 0, len(e.Items))
		for _, one := range e.Items {
			items = append(items, b.mapExprToStorage(one))
		}
		return data.OrExpr{Items: items}
	case data.NotExpr:
		return data.NotExpr{Item: b.mapExprToStorage(e.Item)}
	case data.ExistsExpr:
		return data.ExistsExpr{Field: b.storageField(e.Field), Yes: e.Yes}
	case data.NullExpr:
		return data.NullExpr{Field: b.storageField(e.Field), Yes: e.Yes}
	case data.RawExpr:
		return e
	case data.CmpExpr:
		v := e.Value
		if rf, ok := v.(data.FieldRef); ok {
			v = data.Ref(b.storageField(string(rf)))
		}
		return data.CmpExpr{Field: b.storageField(e.Field), Op: e.Op, Value: v}
	default:
		return e
	}
}

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
		dbName = "infrago"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientOpt := options.Client().ApplyURI(dsn)
	if c.instance.Config.MaxOpen > 0 {
		clientOpt.SetMaxPoolSize(uint64(c.instance.Config.MaxOpen))
	}
	if c.instance.Config.MaxIdleTime > 0 {
		clientOpt.SetMaxConnIdleTime(c.instance.Config.MaxIdleTime)
	}
	cli, err := mongo.Connect(ctx, clientOpt)
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
	return &mongoBase{inst: inst, conn: c, mode: mongoErrorModeFromSetting(inst.Config.Setting)}
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
func (b *mongoBase) WithContext(ctx context.Context) data.DataBase {
	if ctx == nil {
		ctx = context.Background()
	}
	b.mutex.Lock()
	b.ctx = ctx
	b.mutex.Unlock()
	return b
}
func (b *mongoBase) WithTimeout(timeout time.Duration) data.DataBase {
	b.mutex.Lock()
	b.tmo = timeout
	b.mutex.Unlock()
	return b
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

func (b *mongoBase) TxReadOnly(fn data.TxFunc) error {
	return b.Tx(fn)
}
func (b *mongoBase) setError(err error) {
	b.mutex.Lock()
	if err == nil && b.mode == "sticky" {
		b.mutex.Unlock()
		return
	}
	b.err = err
	b.mutex.Unlock()
}
func (b *mongoBase) Error() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	err := b.err
	if b.mode == "auto-clear" {
		b.err = nil
	}
	return err
}
func (b *mongoBase) ClearError() { b.setError(nil) }
func (b *mongoBase) opContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	b.mutex.RLock()
	sc := b.txCtx
	base := b.ctx
	tmo := b.tmo
	b.mutex.RUnlock()
	if base == nil {
		base = context.Background()
	}
	if sc != nil {
		base = sc
	}
	if tmo > 0 {
		timeout = tmo
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
		return b.AggregateRaw(parts[1], firstArg(args))
	case strings.HasPrefix(lower, "find "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			b.setError(fmt.Errorf("raw find requires collection name"))
			return nil
		}
		if len(args) > 1 {
			if opt, ok := args[1].(Map); ok {
				return b.FindRaw(parts[1], firstArg(args), opt)
			}
		}
		return b.FindRaw(parts[1], firstArg(args))
	default:
		command, err := parseCommand(query, firstArg(args))
		if err != nil {
			b.setError(err)
			return nil
		}
		row := b.Command(command)
		if b.Error() != nil || row == nil {
			return nil
		}
		return []Map{row}
	}
}
func (b *mongoBase) Exec(query string, args ...Any) int64 {
	cmd := strings.TrimSpace(query)
	lower := strings.ToLower(cmd)
	if isWriteMongoCommand(lower) {
		if err := b.ensureWritable("exec"); err != nil {
			b.setError(err)
			return 0
		}
	}
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
		if b.fieldMappingEnabled() {
			filter = bson.M(b.toStorageMap(Map(filter)))
		}
		var update bson.M
		if len(args) > 1 {
			if m, ok := args[1].(Map); ok {
				update = buildUpdateDoc(b, m)
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
			docs = append(docs, bson.M(b.toStorageMap(row)))
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
		row := b.Command(command)
		if b.Error() != nil || row == nil {
			return 0
		}
		return 1
	}
}

func (b *mongoBase) ensureWritable(op string) error {
	if b == nil || b.inst == nil {
		return nil
	}
	if b.inst.Config.ReadOnly || isReadOnlyMongoSetting(b.inst.Config.Setting) {
		return data.Error(op, data.ErrValidation, fmt.Errorf("readonly data connection: %s", b.inst.Name))
	}
	return nil
}

func isReadOnlyMongoSetting(setting Map) bool {
	if setting == nil {
		return false
	}
	for _, key := range []string{"readOnly", "readonly"} {
		if raw, ok := setting[key]; ok {
			if vv, ok := parseBool(raw); ok && vv {
				return true
			}
		}
	}
	return false
}

func isWriteMongoCommand(cmd string) bool {
	switch {
	case strings.HasPrefix(cmd, "createcollection "),
		strings.HasPrefix(cmd, "dropcollection "),
		strings.HasPrefix(cmd, "deletemany "),
		strings.HasPrefix(cmd, "updatemany "),
		strings.HasPrefix(cmd, "insertmany "):
		return true
	default:
		return false
	}
}

func (b *mongoBase) Command(cmd Any) Map {
	command, err := parseCommand("command", cmd)
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
	return bsonToMap(row)
}

func (b *mongoBase) FindRaw(collection string, filter Any, opts ...Map) []Map {
	f, err := toBsonMap(filter)
	if err != nil {
		b.setError(err)
		return nil
	}
	findOpts := options.Find()
	if len(opts) > 0 {
		m := opts[0]
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
			findOpts.SetSort(sd)
		}
		if lim, ok := parseInt64(m["limit"]); ok && lim > 0 {
			findOpts.SetLimit(lim)
		}
		if off, ok := parseInt64(m["offset"]); ok && off > 0 {
			findOpts.SetSkip(off)
		}
	}
	ctx, cancel := b.opContext(20 * time.Second)
	defer cancel()
	cur, err := b.conn.db.Collection(collection).Find(ctx, f, findOpts)
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
}

func (b *mongoBase) AggregateRaw(collection string, pipeline Any) []Map {
	pipe, err := parsePipelineArg(pipeline)
	if err != nil {
		b.setError(err)
		return nil
	}
	ctx, cancel := b.opContext(20 * time.Second)
	defer cancel()
	cur, err := b.conn.db.Collection(collection).Aggregate(ctx, pipe)
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
}

func (b *mongoBase) Migrate(names ...string) {
	if err := b.ensureWritable("migrate"); err != nil {
		b.setError(err)
		return
	}
	_, _ = b.migrateWith(names, data.MigrateOptions{})
}

func (b *mongoBase) MigratePlan(names ...string) data.MigrateReport {
	report, _ := b.migrateWith(names, data.MigrateOptions{DryRun: true})
	return report
}

func (b *mongoBase) MigrateDiff(names ...string) data.MigrateReport {
	report, _ := b.migrateWith(names, data.MigrateOptions{DryRun: true, DiffOnly: true})
	return report
}

func (b *mongoBase) MigrateUp(versions ...string) {
	if err := b.ensureWritable("migrate.up"); err != nil {
		b.setError(err)
		return
	}
	b.setError(b.runVersionedUp(versions...))
}

func (b *mongoBase) MigrateDown(steps int) {
	if err := b.ensureWritable("migrate.down"); err != nil {
		b.setError(err)
		return
	}
	b.setError(b.runVersionedDown(steps))
}

func (b *mongoBase) MigrateTo(version string) {
	if err := b.ensureWritable("migrate.to"); err != nil {
		b.setError(err)
		return
	}
	b.setError(b.runVersionedTo(version))
}

func (b *mongoBase) MigrateDownTo(version string) {
	if err := b.ensureWritable("migrate.downTo"); err != nil {
		b.setError(err)
		return
	}
	b.setError(b.runVersionedDownTo(version))
}

func (b *mongoBase) migrateWith(names []string, override data.MigrateOptions) (data.MigrateReport, error) {
	opts := b.inst.Config.Migrate
	if opts.Mode == "" {
		opts.Mode = "safe"
	}
	if override.Mode != "" {
		opts.Mode = strings.ToLower(strings.TrimSpace(override.Mode))
	}
	if override.DryRun {
		opts.DryRun = true
	}
	if override.DiffOnly {
		opts.DiffOnly = true
		opts.DryRun = true
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Minute
	}
	if opts.LockTimeout <= 0 {
		opts.LockTimeout = 30 * time.Second
	}
	if opts.Retry < 0 {
		opts.Retry = 0
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 500 * time.Millisecond
	}
	if opts.Jitter <= 0 {
		opts.Jitter = 250 * time.Millisecond
	}

	report := data.MigrateReport{
		Mode:    opts.Mode,
		DryRun:  opts.DryRun,
		Actions: make([]data.MigrateAction, 0, 8),
	}

	targets := names
	explicit := len(targets) > 0
	if len(targets) == 0 {
		for name := range data.Tables() {
			targets = append(targets, name)
		}
	}
	sort.Strings(targets)
	if !opts.DryRun && opts.Jitter > 0 {
		time.Sleep(time.Duration(time.Now().UnixNano() % int64(opts.Jitter)))
	}
	unlock := func() {}
	if !opts.DryRun {
		u, err := b.acquireMigrateLock(opts)
		if err != nil {
			b.setError(err)
			return report, err
		}
		unlock = u
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()
	for _, name := range targets {
		t, ok := resolveTable(b.inst.Name, name)
		if !ok {
			if explicit {
				err := fmt.Errorf("data table not found: %s", name)
				b.setError(err)
				return report, err
			}
			continue
		}
		source := pickName(name, t.Table)
		cols, err := b.conn.db.ListCollectionNames(ctx, bson.M{"name": source})
		if err != nil {
			b.setError(err)
			return report, err
		}
		if len(cols) == 0 {
			report.Actions = append(report.Actions, data.MigrateAction{
				Kind:   "create_collection",
				Target: source,
				Apply:  !opts.DryRun,
				Risk:   "low",
			})
			if !opts.DryRun {
				if err := b.migrateRetry(opts, func() error { return b.conn.db.CreateCollection(ctx, source) }); err != nil {
					b.setError(err)
					return report, err
				}
			}
		}
		indexes := b.collectIndexes(source, t)
		exists, err := b.loadIndexNames(ctx, source)
		if err != nil {
			b.setError(err)
			return report, err
		}
		for _, idx := range indexes {
			name := ""
			if idx.Options != nil && idx.Options.Name != nil {
				name = strings.TrimSpace(*idx.Options.Name)
			}
			if name == "" {
				continue
			}
			if _, ok := exists[strings.ToLower(name)]; ok {
				continue
			}
			report.Actions = append(report.Actions, data.MigrateAction{
				Kind:   "create_index",
				Target: name,
				Apply:  !opts.DryRun,
				Risk:   "low",
			})
			if !opts.DryRun {
				if err := b.migrateRetry(opts, func() error {
					_, err := b.conn.db.Collection(source).Indexes().CreateOne(ctx, idx)
					return err
				}); err != nil {
					b.setError(err)
					return report, err
				}
			}
		}
	}
	b.setError(nil)
	return report, nil
}

func (b *mongoBase) runVersionedUp(versions ...string) error {
	all := data.Migrations(b.inst.Name)
	if len(all) == 0 {
		return nil
	}
	allow := map[string]struct{}{}
	if len(versions) > 0 {
		for _, v := range versions {
			allow[strings.TrimSpace(v)] = struct{}{}
		}
	}
	opts := b.inst.Config.Migrate
	if opts.LockTimeout <= 0 {
		opts.LockTimeout = 30 * time.Second
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 500 * time.Millisecond
	}
	if opts.Jitter <= 0 {
		opts.Jitter = 250 * time.Millisecond
	}
	unlock, err := b.acquireMigrateLock(opts)
	if err != nil {
		return err
	}
	defer unlock()

	applied, err := b.loadVersionApplied()
	if err != nil {
		return err
	}
	for _, mg := range all {
		if len(allow) > 0 {
			if _, ok := allow[mg.Version]; !ok {
				continue
			}
		}
		if c, ok := applied[mg.Version]; ok {
			if c != mg.Checksum() {
				return fmt.Errorf("migration checksum mismatch: %s", mg.Version)
			}
			continue
		}
		if mg.Up == nil {
			return fmt.Errorf("migration up not defined: %s", mg.Version)
		}
		if err := b.Tx(func(tx data.DataBase) error { return mg.Up(tx) }); err != nil {
			return err
		}
		if err := b.markVersionApplied(mg); err != nil {
			return err
		}
	}
	return nil
}

func (b *mongoBase) runVersionedDown(steps int) error {
	if steps <= 0 {
		steps = 1
	}
	opts := b.inst.Config.Migrate
	if opts.LockTimeout <= 0 {
		opts.LockTimeout = 30 * time.Second
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 500 * time.Millisecond
	}
	if opts.Jitter <= 0 {
		opts.Jitter = 250 * time.Millisecond
	}
	unlock, err := b.acquireMigrateLock(opts)
	if err != nil {
		return err
	}
	defer unlock()

	applied, err := b.loadVersionAppliedOrderedDesc()
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return nil
	}
	mm := map[string]data.Migration{}
	for _, mg := range data.Migrations(b.inst.Name) {
		mm[mg.Version] = mg
	}
	count := 0
	for _, v := range applied {
		if count >= steps {
			break
		}
		mg, ok := mm[v]
		if !ok {
			return fmt.Errorf("migration version not registered: %s", v)
		}
		if mg.Down == nil {
			return fmt.Errorf("migration down not defined: %s", v)
		}
		if err := b.Tx(func(tx data.DataBase) error { return mg.Down(tx) }); err != nil {
			return err
		}
		if err := b.unmarkVersionApplied(v); err != nil {
			return err
		}
		count++
	}
	return nil
}

func (b *mongoBase) runVersionedTo(target string) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return fmt.Errorf("empty migrate target version")
	}
	all := data.Migrations(b.inst.Name)
	if len(all) == 0 {
		return nil
	}
	applied, err := b.loadVersionApplied()
	if err != nil {
		return err
	}
	allow := make([]string, 0, 8)
	found := false
	for _, mg := range all {
		if mg.Version == target {
			found = true
		}
		if _, ok := applied[mg.Version]; ok {
			continue
		}
		allow = append(allow, mg.Version)
		if mg.Version == target {
			break
		}
	}
	if !found {
		return fmt.Errorf("migration target version not found: %s", target)
	}
	if len(allow) == 0 {
		return nil
	}
	return b.runVersionedUp(allow...)
}

func (b *mongoBase) runVersionedDownTo(target string) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return fmt.Errorf("empty migrate down target version")
	}
	all := data.Migrations(b.inst.Name)
	indexes := map[string]int{}
	targetIdx := -1
	for i, mg := range all {
		v := strings.TrimSpace(mg.Version)
		indexes[v] = i
		if v == target {
			targetIdx = i
		}
	}
	if targetIdx < 0 {
		return fmt.Errorf("migration target version not found: %s", target)
	}
	applied, err := b.loadVersionAppliedOrderedDesc()
	if err != nil {
		return err
	}
	steps := 0
	for _, v := range applied {
		idx, ok := indexes[v]
		if !ok {
			return fmt.Errorf("migration version not registered: %s", v)
		}
		if idx > targetIdx {
			steps++
		}
	}
	if steps <= 0 {
		return nil
	}
	return b.runVersionedDown(steps)
}

func (b *mongoBase) acquireMigrateLock(opts data.MigrateOptions) (func(), error) {
	lockColl := b.conn.db.Collection("_infrago_migrate_lock")
	key := b.inst.Name
	deadline := time.Now().Add(opts.LockTimeout)
	for {
		_, err := lockColl.InsertOne(context.Background(), bson.M{
			"_id":       key,
			"createdAt": time.Now(),
		})
		if err == nil {
			return func() {
				_, _ = lockColl.DeleteOne(context.Background(), bson.M{"_id": key})
			}, nil
		}
		msg := strings.ToLower(err.Error())
		if !strings.Contains(msg, "duplicate key") && !strings.Contains(msg, "e11000") {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("migrate lock timeout after %s", opts.LockTimeout)
		}
		time.Sleep(opts.RetryDelay + time.Duration(time.Now().UnixNano()%int64(opts.Jitter)))
	}
}

func (b *mongoBase) migrateRetry(opts data.MigrateOptions, run func() error) error {
	try := opts.Retry + 1
	if try < 1 {
		try = 1
	}
	var last error
	for i := 0; i < try; i++ {
		if err := run(); err == nil {
			return nil
		} else {
			last = err
		}
		if i >= try-1 {
			break
		}
		time.Sleep(opts.RetryDelay + time.Duration(time.Now().UnixNano()%int64(opts.Jitter)))
	}
	return last
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
	items := b.collectIndexes(source, table)
	if len(items) == 0 {
		return nil
	}
	_, err := b.conn.db.Collection(source).Indexes().CreateMany(ctx, items)
	return err
}

func (b *mongoBase) collectIndexes(source string, table data.Table) []mongo.IndexModel {
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
	return items
}

func (b *mongoBase) loadIndexNames(ctx context.Context, source string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	cur, err := b.conn.db.Collection(source).Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		m := bson.M{}
		if err := cur.Decode(&m); err != nil {
			return nil, err
		}
		name, _ := m["name"].(string)
		if strings.TrimSpace(name) != "" {
			out[strings.ToLower(name)] = struct{}{}
		}
	}
	return out, cur.Err()
}

func (b *mongoBase) versionColl() *mongo.Collection {
	return b.conn.db.Collection("_infrago_migrations_v2")
}

func (b *mongoBase) loadVersionApplied() (map[string]string, error) {
	ctx, cancel := b.opContext(10 * time.Second)
	defer cancel()
	cur, err := b.versionColl().Find(ctx, bson.M{})
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "namespace") && strings.Contains(msg, "not found") {
			return map[string]string{}, nil
		}
		return nil, err
	}
	defer cur.Close(ctx)
	out := map[string]string{}
	for cur.Next(ctx) {
		m := bson.M{}
		if err := cur.Decode(&m); err != nil {
			return nil, err
		}
		v, _ := m["version"].(string)
		c, _ := m["checksum"].(string)
		if strings.TrimSpace(v) != "" {
			out[v] = c
		}
	}
	return out, cur.Err()
}

func (b *mongoBase) loadVersionAppliedOrderedDesc() ([]string, error) {
	ctx, cancel := b.opContext(10 * time.Second)
	defer cancel()
	opts := options.Find().SetSort(bson.D{{Key: "appliedAt", Value: -1}, {Key: "version", Value: -1}})
	cur, err := b.versionColl().Find(ctx, bson.M{}, opts)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "namespace") && strings.Contains(msg, "not found") {
			return []string{}, nil
		}
		return nil, err
	}
	defer cur.Close(ctx)
	out := make([]string, 0)
	for cur.Next(ctx) {
		m := bson.M{}
		if err := cur.Decode(&m); err != nil {
			return nil, err
		}
		v, _ := m["version"].(string)
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out, cur.Err()
}

func (b *mongoBase) markVersionApplied(mg data.Migration) error {
	ctx, cancel := b.opContext(10 * time.Second)
	defer cancel()
	_, err := b.versionColl().UpdateOne(ctx, bson.M{"version": mg.Version}, bson.M{
		"$set": bson.M{
			"version":   mg.Version,
			"name":      mg.Name,
			"checksum":  mg.Checksum(),
			"appliedAt": time.Now(),
		},
	}, options.Update().SetUpsert(true))
	return err
}

func (b *mongoBase) unmarkVersionApplied(version string) error {
	ctx, cancel := b.opContext(10 * time.Second)
	defer cancel()
	_, err := b.versionColl().DeleteOne(ctx, bson.M{"version": version})
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

func (t *mongoTable) Insert(dataIn Map) Map {
	if err := t.base.ensureWritable(t.name + ".insert"); err != nil {
		t.base.setError(err)
		return nil
	}
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	doc := bson.M(t.base.toStorageMap(dataIn))
	res, err := t.coll().InsertOne(ctx, doc)
	if err != nil {
		t.base.setError(err)
		return nil
	}
	out := cloneMap(dataIn)
	if _, ok := out[t.key]; !ok {
		out[t.key] = res.InsertedID
	}
	if out[t.key] != nil {
		if entity := t.First(Map{t.key: out[t.key]}); t.base.Error() == nil && entity != nil {
			out = entity
		} else {
			t.base.setError(nil)
		}
	}
	data.TouchTableCache(t.base.inst.Name, t.source)
	data.EmitMutation(t.base.inst.Name, t.source, data.MutationInsert, 1, out[t.key], out, nil)
	t.base.setError(nil)
	return out
}

func (t *mongoTable) InsertMany(items []Map) []Map {
	if err := t.base.ensureWritable(t.name + ".insertMany"); err != nil {
		t.base.setError(err)
		return nil
	}
	if len(items) == 0 {
		t.base.setError(nil)
		return []Map{}
	}
	ctx, cancel := t.base.opContext(15 * time.Second)
	defer cancel()
	docs := make([]any, 0, len(items))
	for _, item := range items {
		docs = append(docs, bson.M(t.base.toStorageMap(item)))
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
	data.TouchTableCache(t.base.inst.Name, t.source)
	data.EmitMutation(t.base.inst.Name, t.source, data.MutationInsert, int64(len(out)), nil, nil, nil)
	t.base.setError(nil)
	return out
}

func (t *mongoTable) Upsert(dataIn Map, args ...Any) Map {
	if err := t.base.ensureWritable(t.name + ".upsert"); err != nil {
		t.base.setError(err)
		return nil
	}
	filter := make(Map)
	if len(args) > 0 {
		if m, ok := args[0].(Map); ok {
			for k, v := range m {
				filter[t.base.storageField(k)] = v
			}
		}
	}
	if len(filter) == 0 {
		if id, ok := dataIn[t.key]; ok {
			filter[t.base.storageField(t.key)] = id
		}
	}
	if len(filter) == 0 {
		out := t.Insert(dataIn)
		if t.base.Error() == nil && out != nil {
			data.EmitMutation(t.base.inst.Name, t.source, data.MutationUpsert, 1, out[t.key], nil, filter)
		}
		return out
	}
	upd := buildUpdateDoc(t.base, dataIn)
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	_, err := t.coll().UpdateOne(ctx, bson.M(filter), upd, options.Update().SetUpsert(true))
	if err != nil {
		t.base.setError(err)
		return nil
	}
	data.TouchTableCache(t.base.inst.Name, t.source)
	data.EmitMutation(t.base.inst.Name, t.source, data.MutationUpsert, 1, nil, nil, filter)
	out := t.First(filter)
	return out
}

func (t *mongoTable) UpsertMany(items []Map, args ...Any) []Map {
	if err := t.base.ensureWritable(t.name + ".upsertMany"); err != nil {
		t.base.setError(err)
		return nil
	}
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
	if err := t.base.ensureWritable(t.name + ".change"); err != nil {
		t.base.setError(err)
		return nil
	}
	if item == nil || item[t.key] == nil {
		t.base.setError(fmt.Errorf("missing primary key %s", t.key))
		return nil
	}
	upd := buildUpdateDoc(t.base, dataIn)
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	_, err := t.coll().UpdateOne(ctx, bson.M{t.base.storageField(t.key): item[t.key]}, upd)
	if err != nil {
		t.base.setError(err)
		return nil
	}
	data.TouchTableCache(t.base.inst.Name, t.source)
	data.EmitMutation(t.base.inst.Name, t.source, data.MutationUpdate, 1, item[t.key], dataIn, Map{t.key: item[t.key]})
	return t.First(Map{t.key: item[t.key]})
}

func (t *mongoTable) Remove(args ...Any) Map {
	if err := t.base.ensureWritable(t.name + ".remove"); err != nil {
		t.base.setError(err)
		return nil
	}
	item := t.First(args...)
	if t.base.Error() != nil || item == nil {
		return nil
	}
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	_, err := t.coll().DeleteOne(ctx, bson.M{t.base.storageField(t.key): item[t.key]})
	if err != nil {
		t.base.setError(err)
		return nil
	}
	data.TouchTableCache(t.base.inst.Name, t.source)
	data.EmitMutation(t.base.inst.Name, t.source, data.MutationDelete, 1, item[t.key], nil, Map{t.key: item[t.key]})
	t.base.setError(nil)
	return item
}

func (t *mongoTable) Update(sets Map, args ...Any) int64 {
	if err := t.base.ensureWritable(t.name + ".update"); err != nil {
		t.base.setError(err)
		return 0
	}
	q, err := data.Parse(args...)
	if err != nil {
		t.base.setError(err)
		return 0
	}
	q = t.base.mapQueryToStorage(q)
	filter, err := exprToFilter(q.Filter)
	if err != nil {
		t.base.setError(err)
		return 0
	}
	ctx, cancel := t.base.opContext(10 * time.Second)
	defer cancel()
	res, err := t.coll().UpdateMany(ctx, filter, buildUpdateDoc(t.base, sets))
	if err != nil {
		t.base.setError(err)
		return 0
	}
	data.TouchTableCache(t.base.inst.Name, t.source)
	data.EmitMutation(t.base.inst.Name, t.source, data.MutationUpdate, res.ModifiedCount, nil, sets, nil)
	t.base.setError(nil)
	return res.ModifiedCount
}

func (t *mongoTable) Delete(args ...Any) int64 {
	if err := t.base.ensureWritable(t.name + ".delete"); err != nil {
		t.base.setError(err)
		return 0
	}
	q, err := data.Parse(args...)
	if err != nil {
		t.base.setError(err)
		return 0
	}
	q = t.base.mapQueryToStorage(q)
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
	data.TouchTableCache(t.base.inst.Name, t.source)
	data.EmitMutation(t.base.inst.Name, t.source, data.MutationDelete, res.DeletedCount, nil, nil, nil)
	t.base.setError(nil)
	return res.DeletedCount
}

func (t *mongoTable) Entity(id Any) Map           { return t.First(Map{t.key: id}) }
func (t *mongoTable) Count(args ...Any) int64     { return (*mongoView)(t).Count(args...) }
func (t *mongoTable) Aggregate(args ...Any) []Map { return (*mongoView)(t).Aggregate(args...) }
func (t *mongoTable) First(args ...Any) Map       { return (*mongoView)(t).First(args...) }
func (t *mongoTable) Query(args ...Any) []Map     { return (*mongoView)(t).Query(args...) }
func (t *mongoTable) Scan(next data.ScanFunc, args ...Any) Res {
	return (*mongoView)(t).Scan(next, args...)
}
func (t *mongoTable) ScanN(limit int64, next data.ScanFunc, args ...Any) Res {
	return (*mongoView)(t).ScanN(limit, next, args...)
}
func (t *mongoTable) Slice(offset, limit int64, args ...Any) (int64, []Map) {
	return (*mongoView)(t).Slice(offset, limit, args...)
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
	q = v.base.mapQueryToStorage(q)
	if total, ok := v.loadCountCache(q); ok {
		v.base.setError(nil)
		return total
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
	v.storeCountCache(q, total)
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
	q = v.base.mapQueryToStorage(q)
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
	q = v.base.mapQueryToStorage(q)
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
	q = v.base.mapQueryToStorage(q)
	items, err := v.aggregateWithQuery(q)
	v.base.setError(err)
	return items
}

func (v *mongoView) Scan(next data.ScanFunc, args ...Any) Res {
	return v.ScanN(0, next, args...)
}

func (v *mongoView) ScanN(limit int64, next data.ScanFunc, args ...Any) Res {
	if next == nil {
		return nil
	}
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return nil
	}
	q = v.base.mapQueryToStorage(q)
	if limit > 0 {
		q.Limit = limit
	}
	batch := q.Batch
	if batch <= 0 {
		batch = v.base.scanBatchSize()
	}
	if batch > 0 {
		offset := q.Offset
		remain := q.Limit
		for {
			chunk := batch
			if remain > 0 && chunk > remain {
				chunk = remain
			}
			qq := q
			qq.Offset = offset
			qq.Limit = chunk
			items, err := v.queryWithQuery(qq)
			if err != nil {
				v.base.setError(err)
				return nil
			}
			for _, item := range items {
				if res := next(item); res != nil && res.Fail() {
					return res
				}
			}
			if len(items) == 0 || int64(len(items)) < chunk {
				break
			}
			offset += int64(len(items))
			if remain > 0 {
				remain -= int64(len(items))
				if remain <= 0 {
					break
				}
			}
		}
		v.base.setError(nil)
		return nil
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

func (v *mongoView) Slice(offset, limit int64, args ...Any) (int64, []Map) {
	q, err := data.Parse(args...)
	if err != nil {
		v.base.setError(err)
		return 0, nil
	}
	q = v.base.mapQueryToStorage(q)
	q.Offset = offset
	q.Limit = limit
	if len(q.Sort) == 0 {
		key := strings.TrimSpace(v.base.storageField(v.key))
		if key != "" {
			q.Sort = []data.Sort{{Field: key}}
		}
	}
	total := v.Count(args...)
	if v.base.Error() != nil {
		return 0, nil
	}
	items, err := v.queryWithQuery(q)
	if err != nil {
		v.base.setError(err)
		return 0, nil
	}
	v.base.setError(nil)
	return total, items
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
	q = v.base.mapQueryToStorage(q)
	items, err := v.aggregateWithQuery(q)
	v.base.setError(err)
	return items
}

func (m *mongoModel) First(args ...Any) Map   { return m.mongoView.First(args...) }
func (m *mongoModel) Query(args ...Any) []Map { return m.mongoView.Query(args...) }
func (m *mongoModel) Scan(next data.ScanFunc, args ...Any) Res {
	return m.mongoView.Scan(next, args...)
}
func (m *mongoModel) ScanN(limit int64, next data.ScanFunc, args ...Any) Res {
	return m.mongoView.ScanN(limit, next, args...)
}
func (m *mongoModel) Slice(offset, limit int64, args ...Any) (int64, []Map) {
	return m.mongoView.Slice(offset, limit, args...)
}

func (v *mongoView) queryWithQuery(q data.Query) ([]Map, error) {
	if len(q.Aggs) > 0 || len(q.Group) > 0 {
		return v.aggregateWithQuery(q)
	}
	q = applyAfter(q)
	if items, ok := v.loadQueryCache(q); ok {
		return items, nil
	}
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
	if q.Batch > 0 {
		findOpts.SetBatchSize(int32(q.Batch))
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
		out = append(out, v.base.toAppMap(bsonToMap(m)))
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	v.storeQueryCache(q, out)
	return out, nil
}

func (v *mongoView) loadQueryCache(q data.Query) ([]Map, bool) {
	if v == nil || v.base == nil || !v.base.cacheEnabled() {
		return nil, false
	}
	token := data.CacheToken(v.base.inst.Name, v.cacheTables(q))
	key := "q:" + token + ":" + data.QuerySignature(q)
	raw, ok := mongoCacheMap(v.base.inst.Name).Load(key)
	if !ok {
		return nil, false
	}
	cv, ok := raw.(mongoCacheValue)
	if !ok {
		return nil, false
	}
	if cv.expireAt > 0 && time.Now().UnixNano() > cv.expireAt {
		mongoCacheDelete(v.base.inst.Name, key)
		return nil, false
	}
	return cloneMaps(cv.items), true
}

func (v *mongoView) storeQueryCache(q data.Query, items []Map) {
	if v == nil || v.base == nil || !v.base.cacheEnabled() {
		return
	}
	ttl := v.base.cacheTTL()
	if ttl <= 0 {
		return
	}
	token := data.CacheToken(v.base.inst.Name, v.cacheTables(q))
	key := "q:" + token + ":" + data.QuerySignature(q)
	mongoCacheStore(v.base.inst.Name, key, mongoCacheValue{
		expireAt: time.Now().Add(ttl).UnixNano(),
		items:    cloneMaps(items),
		total:    -1,
	}, v.base.cacheCapacity())
}

func (v *mongoView) loadCountCache(q data.Query) (int64, bool) {
	if v == nil || v.base == nil || !v.base.cacheEnabled() {
		return 0, false
	}
	token := data.CacheToken(v.base.inst.Name, v.cacheTables(q))
	key := "c:" + token + ":" + data.QuerySignature(q)
	raw, ok := mongoCacheMap(v.base.inst.Name).Load(key)
	if !ok {
		return 0, false
	}
	cv, ok := raw.(mongoCacheValue)
	if !ok {
		return 0, false
	}
	if cv.expireAt > 0 && time.Now().UnixNano() > cv.expireAt {
		mongoCacheDelete(v.base.inst.Name, key)
		return 0, false
	}
	return cv.total, true
}

func (v *mongoView) storeCountCache(q data.Query, total int64) {
	if v == nil || v.base == nil || !v.base.cacheEnabled() {
		return
	}
	ttl := v.base.cacheTTL()
	if ttl <= 0 {
		return
	}
	token := data.CacheToken(v.base.inst.Name, v.cacheTables(q))
	key := "c:" + token + ":" + data.QuerySignature(q)
	mongoCacheStore(v.base.inst.Name, key, mongoCacheValue{
		expireAt: time.Now().Add(ttl).UnixNano(),
		total:    total,
	}, v.base.cacheCapacity())
}

func (v *mongoView) cacheTables(q data.Query) []string {
	out := make([]string, 0, len(q.Joins)+1)
	out = append(out, v.source)
	for _, join := range q.Joins {
		if strings.TrimSpace(join.From) != "" {
			out = append(out, join.From)
		}
	}
	return out
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
		out = append(out, v.base.toAppMap(flat))
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
	case OpEq:
		return bson.M{field: normalizeCmpValue(value)}, nil
	case OpNe:
		return bson.M{field: bson.M{"$ne": normalizeCmpValue(value)}}, nil
	case OpGt:
		return bson.M{field: bson.M{"$gt": normalizeCmpValue(value)}}, nil
	case OpGte:
		return bson.M{field: bson.M{"$gte": normalizeCmpValue(value)}}, nil
	case OpLt:
		return bson.M{field: bson.M{"$lt": normalizeCmpValue(value)}}, nil
	case OpLte:
		return bson.M{field: bson.M{"$lte": normalizeCmpValue(value)}}, nil
	case OpIn:
		return bson.M{field: bson.M{"$in": toAnySlice(value)}}, nil
	case OpNin:
		return bson.M{field: bson.M{"$nin": toAnySlice(value)}}, nil
	case OpLike:
		return bson.M{field: primitive.Regex{Pattern: likeToRegex(fmt.Sprintf("%v", value)), Options: ""}}, nil
	case OpILike:
		return bson.M{field: primitive.Regex{Pattern: likeToRegex(fmt.Sprintf("%v", value)), Options: "i"}}, nil
	case OpRegex:
		return bson.M{field: primitive.Regex{Pattern: fmt.Sprintf("%v", value), Options: "i"}}, nil
	case OpContains:
		s := toAnySlice(value)
		if len(s) > 0 {
			return bson.M{field: bson.M{"$all": s}}, nil
		}
		if m, ok := value.(Map); ok {
			return bson.M{field: bson.M{"$all": []any{m}}}, nil
		}
		return bson.M{field: value}, nil
	case OpOverlap:
		return bson.M{field: bson.M{"$in": toAnySlice(value)}}, nil
	case OpElemMatch:
		if m, ok := value.(Map); ok {
			return bson.M{field: bson.M{"$elemMatch": bson.M(m)}}, nil
		}
		return bson.M{field: bson.M{"$elemMatch": value}}, nil
	default:
		return nil, fmt.Errorf("unsupported compare operator %s", c.Op)
	}
}

func buildUpdateDoc(base *mongoBase, input Map) bson.M {
	setPart := bson.M{}
	incPart := bson.M{}
	unsetPart := bson.M{}
	pushPart := bson.M{}
	pullPart := bson.M{}
	addSetPart := bson.M{}
	field := func(name string) string {
		if base == nil {
			return name
		}
		return base.storageField(name)
	}

	for k, v := range input {
		switch k {
		case UpdSet:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					setPart[field(kk)] = vv
				}
			}
		case UpdInc:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					incPart[field(kk)] = vv
				}
			}
		case UpdUnset:
			switch vv := v.(type) {
			case string:
				unsetPart[field(vv)] = ""
			case []string:
				for _, one := range vv {
					unsetPart[field(one)] = ""
				}
			case []Any:
				for _, one := range vv {
					if s, ok := one.(string); ok {
						unsetPart[field(s)] = ""
					}
				}
			case Map:
				for kk := range vv {
					unsetPart[field(kk)] = ""
				}
			}
		case UpdPush:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					arr := toAnySlice(vv)
					if len(arr) > 1 {
						pushPart[field(kk)] = bson.M{"$each": arr}
					} else if len(arr) == 1 {
						pushPart[field(kk)] = arr[0]
					}
				}
			}
		case UpdPull:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					arr := toAnySlice(vv)
					if len(arr) > 1 {
						pullPart[field(kk)] = bson.M{"$in": arr}
					} else if len(arr) == 1 {
						pullPart[field(kk)] = arr[0]
					}
				}
			}
		case UpdAddToSet:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					arr := toAnySlice(vv)
					if len(arr) > 1 {
						addSetPart[field(kk)] = bson.M{"$each": arr}
					} else if len(arr) == 1 {
						addSetPart[field(kk)] = arr[0]
					}
				}
			}
		case UpdSetPath:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					setPart[field(kk)] = vv
				}
			}
		case UpdUnsetPath:
			switch vv := v.(type) {
			case string:
				unsetPart[field(vv)] = ""
			case []string:
				for _, one := range vv {
					unsetPart[field(one)] = ""
				}
			case []Any:
				for _, one := range vv {
					if s, ok := one.(string); ok {
						unsetPart[field(s)] = ""
					}
				}
			}
		default:
			if !strings.HasPrefix(k, "$") {
				setPart[field(k)] = v
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
	op := OpGt
	if sf.Desc {
		op = OpLt
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
		case OpEq:
			return bson.M{"$eq": []Any{left, right}}, nil
		case OpNe:
			return bson.M{"$ne": []Any{left, right}}, nil
		case OpGt:
			return bson.M{"$gt": []Any{left, right}}, nil
		case OpGte:
			return bson.M{"$gte": []Any{left, right}}, nil
		case OpLt:
			return bson.M{"$lt": []Any{left, right}}, nil
		case OpLte:
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

func mongoErrorModeFromSetting(setting Map) string {
	mode := "auto-clear"
	if setting == nil {
		return mode
	}
	if raw, ok := setting["errorMode"]; ok {
		if s, ok := raw.(string); ok {
			v := strings.ToLower(strings.TrimSpace(s))
			if v == "sticky" || v == "auto-clear" {
				return v
			}
		}
	}
	return mode
}

func (b *mongoBase) scanBatchSize() int64 {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return 0
	}
	for _, key := range []string{"scanBatch", "scan_batch"} {
		if raw, ok := b.inst.Config.Setting[key]; ok {
			if vv, ok := parseInt64(raw); ok && vv > 0 {
				return vv
			}
		}
	}
	return 0
}

func (b *mongoBase) cacheEnabled() bool {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return false
	}
	raw, ok := b.inst.Config.Setting["cache"]
	if !ok {
		return false
	}
	switch vv := raw.(type) {
	case bool:
		return vv
	case int:
		return vv > 0
	case int64:
		return vv > 0
	case string:
		vv = strings.TrimSpace(strings.ToLower(vv))
		return vv == "true" || vv == "1" || vv == "yes"
	case Map:
		if e, ok := vv["enable"]; ok {
			if yes, ok := parseBool(e); ok {
				return yes
			}
		}
		return true
	default:
		return false
	}
}

func (b *mongoBase) cacheTTL() time.Duration {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return 0
	}
	raw, ok := b.inst.Config.Setting["cache"]
	if !ok {
		return 0
	}
	switch vv := raw.(type) {
	case Map:
		if d, ok := vv["ttl"]; ok {
			switch dt := d.(type) {
			case string:
				parsed, err := time.ParseDuration(strings.TrimSpace(dt))
				if err == nil && parsed > 0 {
					return parsed
				}
			case int:
				if dt > 0 {
					return time.Second * time.Duration(dt)
				}
			case int64:
				if dt > 0 {
					return time.Second * time.Duration(dt)
				}
			}
		}
	}
	return 3 * time.Second
}

func (b *mongoBase) cacheCapacity() int {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return 0
	}
	raw, ok := b.inst.Config.Setting["cache"]
	if !ok {
		return 0
	}
	if vv, ok := raw.(Map); ok {
		for _, key := range []string{"capacity", "cap", "max"} {
			if c, ok := vv[key]; ok {
				if n, yes := parseInt64(c); yes && n > 0 {
					return int(n)
				}
			}
		}
	}
	return 0
}

func mongoCacheMap(name string) *sync.Map {
	if strings.TrimSpace(name) == "" {
		name = "default"
	}
	if v, ok := mongoCacheRegistry.Load(name); ok {
		return v.(*sync.Map)
	}
	m := &sync.Map{}
	actual, _ := mongoCacheRegistry.LoadOrStore(name, m)
	return actual.(*sync.Map)
}

func mongoCacheCountPtr(name string) *atomic.Int64 {
	if strings.TrimSpace(name) == "" {
		name = "default"
	}
	if v, ok := mongoCacheCount.Load(name); ok {
		return v.(*atomic.Int64)
	}
	p := &atomic.Int64{}
	actual, _ := mongoCacheCount.LoadOrStore(name, p)
	return actual.(*atomic.Int64)
}

func mongoCacheDelete(name, key string) {
	if _, ok := mongoCacheMap(name).LoadAndDelete(key); ok {
		mongoCacheCountPtr(name).Add(-1)
	}
}

func mongoCacheStore(name, key string, val mongoCacheValue, capacity int) {
	if _, loaded := mongoCacheMap(name).Load(key); !loaded {
		mongoCacheCountPtr(name).Add(1)
	}
	mongoCacheMap(name).Store(key, val)
	if capacity <= 0 {
		return
	}
	for {
		if mongoCacheCountPtr(name).Load() <= int64(capacity) {
			break
		}
		removed := false
		mongoCacheMap(name).Range(func(k, _ any) bool {
			s, ok := k.(string)
			if !ok || strings.TrimSpace(s) == "" {
				return true
			}
			mongoCacheDelete(name, s)
			removed = true
			return false
		})
		if !removed {
			break
		}
	}
}

func cloneMaps(items []Map) []Map {
	out := make([]Map, 0, len(items))
	for _, item := range items {
		out = append(out, cloneMap(item))
	}
	return out
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
