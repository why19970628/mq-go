package lib

import (
	"fmt"
	"github.com/aliyunmq/mq-http-go-sdk"
	"github.com/gogap/errors"
	"log"
	"strings"
	"time"
)

var (
	mqTransProducer mq_http_sdk.MQTransProducer
	mqConsumer      mq_http_sdk.MQConsumer
)

func init() {
	// 设置HTTP协议客户端接入点，进入消息队列RocketMQ版控制台实例详情页面的接入点区域查看。
	endpoint := "${HTTP_ENDPOINT}"
	// AccessKey ID阿里云身份验证，在阿里云RAM控制台创建。
	accessKey := "${ACCESS_KEY}"
	// AccessKey Secret阿里云身份验证，在阿里云RAM控制台创建。
	secretKey := "${SECRET_KEY}"
	// 消息所属的Topic，在消息队列RocketMQ版控制台创建。
	//不同消息类型的Topic不能混用，例如普通消息的Topic只能用于收发普通消息，不能用于收发其他类型的消息。
	topic := "${TOPIC}"
	// Topic所属的实例ID，在消息队列RocketMQ版控制台创建。
	// 若实例有命名空间，则实例ID必须传入；若实例无命名空间，则实例ID传入null空值或字符串空值。实例的命名空间可以在消息队列RocketMQ版控制台的实例详情页面查看。
	instanceId := "${INSTANCE_ID}"
	// 您在控制台创建的Group ID。
	groupId := "${GROUP_ID}"

	client := mq_http_sdk.NewAliyunMQClient(endpoint, accessKey, secretKey, "")

	mqTransProducer = client.GetTransProducer(instanceId, topic, groupId)
	mqConsumer = client.GetConsumer(instanceId, topic, groupId, "")
	go ConsumeHalfMsg(&mqTransProducer)
	go Consumer()
	//select {}
}

// 发送普通消息
func PublishMessage(body string) {
	msg := mq_http_sdk.PublishMessageRequest{
		MessageBody: body,
		//Properties:  map[string]string{"a": strconv.Itoa(i)},
	}
	// 设置事务第一次回查的时间，为相对时间，单位：秒，范围为10~300s之间
	// 第一次事务回查后如果消息没有commit或者rollback，则之后每隔10s左右会回查一次，总共回查一天
	msg.TransCheckImmunityTime = 10
	resp, pubErr := mqTransProducer.PublishMessage(msg)
	if pubErr != nil {
		fmt.Println(pubErr)
	}
	fmt.Printf("Publish ---->\n\tMessageId:%s, BodyMD5:%s, Handle:%s\n",
		resp.MessageId, resp.MessageBodyMD5, resp.ReceiptHandle)
}

func ProcessError(err error) {
	// 如果Commit/Rollback时超过了TransCheckImmunityTime（针对发送事务消息的句柄）或者超过10s（针对consumeHalfMessage的句柄）则会失败
	if err == nil {
		return
	}
	fmt.Println(err)
	for _, errAckItem := range err.(errors.ErrCode).Context()["Detail"].([]mq_http_sdk.ErrAckItem) {
		fmt.Printf("\tErrorHandle:%s, ErrorCode:%s, ErrorMsg:%s\n",
			errAckItem.ErrorHandle, errAckItem.ErrorCode, errAckItem.ErrorMsg)
	}
}

func CreateUser(id string) {

}

func ConsumeHalfMsg(mqTransProducer *mq_http_sdk.MQTransProducer) {
	for {
		endChan := make(chan int)
		respChan := make(chan mq_http_sdk.ConsumeMessageResponse)
		errChan := make(chan error)
		go func() {
			select {
			case resp := <-respChan:
				{
					// 处理业务逻辑
					var handles []string
					fmt.Printf("HalfMsg Consume %d messages---->\n", len(resp.Messages))
					for _, v := range resp.Messages {
						handles = append(handles, v.ReceiptHandle)
						fmt.Printf("\tMessageID: %s, PublishTime: %d, MessageTag: %s\n"+
							"\tConsumedTimes: %d, FirstConsumeTime: %d, NextConsumeTime: %d\n\tBody: %s\n"+
							"\tProperties:%s, Key:%s, Timer:%d, Trans:%d\n",
							v.MessageId, v.PublishTime, v.MessageTag, v.ConsumedTimes,
							v.FirstConsumeTime, v.NextConsumeTime, v.MessageBody,
							v.Properties, v.MessageKey, v.StartDeliverTime, v.TransCheckImmunityTime)

						var comRollErr error
						// 事务日志表 https://zhuanlan.zhihu.com/p/115553176

						// 状态失败等
						if !QuerySenderTransMsg(v.MessageBody) {
							//	// 确认回滚事务消息
							comRollErr = (*mqTransProducer).Rollback(v.ReceiptHandle)
							fmt.Println("Producer Rollback---------->")
						} else {
							comRollErr = (*mqTransProducer).Commit(v.ReceiptHandle)
							fmt.Println("Producer Commit---------->")
						}
						ProcessError(comRollErr)
					}
					endChan <- 1
				}
			case err := <-errChan:
				{
					// 没有消息
					//if strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {
					//	fmt.Println("\nNo new message, continue!")
					//} else {
					//	fmt.Println(err)
					//	time.Sleep(time.Duration(3) * time.Second)
					//}
					// 没有消息
					if !strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {
						log.Println(err)
						time.Sleep(time.Duration(3) * time.Second)
					}
					endChan <- 1
				}
			case <-time.After(35 * time.Second):
				{
					fmt.Println("Timeout of consumer message ??")
					return
				}
			}
		}()

		// 长轮询检查事务半消息
		// 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
		(*mqTransProducer).ConsumeHalfMessage(respChan, errChan,
			3, // 一次最多消费3条(最多可设置为16条)
			3, // 长轮询时间3秒（最多可设置为30秒）
		)
		<-endChan
	}
}
