package data_mongodb

import (
	"sync"

	"golang.org/x/net/context"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/infrago/data"
)

type (
	//数据库连接
	MongodbConnect struct {
		mutex    sync.RWMutex
		instance *data.Instance
		setting  MongodbSetting

		schema string

		//数据库对象
		client  *mongo.Client
		actives int64
	}

	MongodbSetting struct {
	}
)

// 打开连接
func (this *MongodbConnect) Open() error {
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		return err
	}
	this.client = client
	return nil
}

// 健康检查
func (this *MongodbConnect) Health() (data.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return data.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *MongodbConnect) Close() error {
	if this.client != nil {
		return this.client.Disconnect(context.Background())
	}

	return nil
}

func (this *MongodbConnect) Base() data.DataBase {
	this.mutex.Lock()
	this.actives++
	this.mutex.Unlock()

	return &MongodbBase{this, this.instance.Name, this.schema, false, []mongodbTrigger{}, nil}
}
