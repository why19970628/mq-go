package common

import mq_http_sdk "github.com/aliyunmq/mq-http-go-sdk"

// MQ生产数据
type CustomMQData interface {
	MQTopic() string
	MQTag() string
}

var (
	// 设置HTTP接入域名（此处以公共云生产环境为例）
	MQ_ENDPOINT = "${HTTP_ENDPOINT}"
	// AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
	ACCESSKEY = "${ACCESS_KEY}"
	// SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
	SECRETKEY = "${SECRET_KEY}"
	// 所属的 Topic
	//topic := "${TOPIC}"
	// Topic所属实例ID，默认实例为空
	INSTANCEID = "${INSTANCE_ID}"
	// 您在控制台创建的 Consumer ID(Group ID)
	GroupID = "${GROUP_ID}"
)

func Client() (client mq_http_sdk.MQClient, err error) {
	defer func() {
		if pan := recover(); pan != nil {
		}
	}()
	client = mq_http_sdk.NewAliyunMQClient(MQ_ENDPOINT, ACCESSKEY, SECRETKEY, "")
	return
}
