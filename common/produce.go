package common

import (
	"encoding/json"
	"fmt"
	mq_http_sdk "github.com/aliyunmq/mq-http-go-sdk"
)

// 生产消息
func Produce(data CustomMQData) error {
	return ProduceWithProperties(data, map[string]string{})
}

// 生产消息
func ProduceWithProperties(data CustomMQData, properties map[string]string) error {
	client, err := Client()
	if err != nil || client == nil {
		return err
	}

	topic := data.MQTopic()
	tag := data.MQTag()

	info, _ := json.Marshal(data)
	producer := client.GetTransProducer(INSTANCEID, topic, GroupID)
	msg := mq_http_sdk.PublishMessageRequest{
		MessageBody: string(info), // 消息内容string
		MessageTag:  tag,
		Properties:  properties,
	}
	_, err = producer.PublishMessage(msg)
	if err != nil {
		return fmt.Errorf("MQ发送消息失败: %v", err)
	} else {
		fmt.Println("发送成功")
	}
	return nil
}
