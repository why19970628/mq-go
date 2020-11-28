/**
 * @Author: why19970628
 * @Description: rocketmq simple
 * @File:  RocketMQ
 * @Version: 1.0.0
 * @Date: 2020-11-12 12:02
 */
package common

import (
	"context"
	"fmt"
	"sync"

	mq_http_sdk "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/gogap/errors"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/sirupsen/logrus"
	"gopkg.in/ffmt.v1"
)

var Log *logrus.Logger

var (
	Topic        = "your_topic"
	mqCommonPool *pool.ObjectPool
	once         sync.Once
	ctx          = context.Background()
)

var (
	mq_endpoint = ""
	accessKey   = ""
	secretKey   = ""
)

//MQ数据结构
type MQStruct struct {
	Mq_type    int `json:"mq_type"`
	Mq_dyn_log MQDynLog
}

//动态浏览历史
type MQDynLog struct {
	Uid        string `json:"uid"`
	Dyn_id     string `json:"dyn_id"`
	Visit_time string `json:"visit_time"`
}

func init() {
	Setup()
}

func Setup() {
	once.Do(func() {
		// 初始化连接池配置项
		PoolConfig := pool.NewDefaultPoolConfig()
		// 连接池最大容量设置
		PoolConfig.MaxTotal = 1000
		PoolConfig.MaxIdle = 1000
		WithAbandonedConfig := pool.NewDefaultAbandonedConfig()
		// 注册连接池初始化链接方式
		mqCommonPool = pool.NewObjectPoolWithAbandonedConfig(ctx, pool.NewPooledObjectFactorySimple(
			func(context.Context) (interface{}, error) {
				return MqLink()
			}), PoolConfig, WithAbandonedConfig)
	})
}

// 初始化链接类
func MqLink() (mq_http_sdk.MQClient, error) {
	client := mq_http_sdk.NewAliyunMQClient(mq_endpoint, accessKey, secretKey, "")
	return client, nil
}

func MQClient() (client *mq_http_sdk.AliyunMQClient, err error) {
	// 从连接池中获取一个实例
	obj, err := mqCommonPool.BorrowObject(ctx)
	// 转换为对应实体
	if err != nil {
		data := map[string]interface{}{
			"filename": "es error or client nil",
			"size":     12,
		}
		Log.WithFields(data).Info(err)
	}
	client = obj.(*mq_http_sdk.AliyunMQClient)
	if client == nil {
		//log
		err = errors.New("client 不能为空,MQ参数非法,节点获取timeout")
		data := map[string]interface{}{
			"filename": "es",
			"size":     10,
		}
		Log.WithFields(data).Info(fmt.Sprintf("ES client not created:%v", err))
	}
	defer mqCommonPool.ReturnObject(ctx, client)
	return
}

/*
 *@des 生产数据
 *@ data：json消息（string）  topic：
 */
func MQProduct(data, topic string) error {
	client, err := MQClient()
	mqProducer := client.GetProducer(INSTANCEID, topic)
	msg := mq_http_sdk.PublishMessageRequest{
		MessageBody: data,                //消息内容
		MessageTag:  "",                  // 消息标签
		Properties:  map[string]string{}, // 消息属性
	}
	// 设置KEY
	//msg.MessageKey = "MessageKey"
	//设置属性
	//msg.Properties["a"] = strconv.Itoa(1)

	ret, err := mqProducer.PublishMessage(msg)
	if err != nil {
		ffmt.Puts(err)
		return err
	}
	ffmt.Puts(ret)
	return nil
}
