package common

import (
	"fmt"
	"github.com/gogap/errors"
	"strings"
	"time"

	"github.com/aliyunmq/mq-http-go-sdk"
)

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
	GROUPID = "${GROUP_ID}"
)

//指定消费消息返回
func ConsumeContent(topic string) (res bool, exchange []string) {
	client := mq_http_sdk.NewAliyunMQClient(MQ_ENDPOINT, ACCESSKEY, SECRETKEY, "")
	mqConsumer := client.GetConsumer(INSTANCEID, topic, GROUPID, "")
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
					for _, v := range resp.Messages {
						handles = append(handles, v.ReceiptHandle)

						exchange = append(exchange, v.MessageBody)
					}
					// NextConsumeTime前若不确认消息消费成功，则消息会重复消费
					// 消息句柄有时间戳，同一条消息每次消费拿到的都不一样
					ackerr := mqConsumer.AckMessage(handles)
					if ackerr != nil {
						// 某些消息的句柄可能超时了会导致确认不成功
						fmt.Println(ackerr)
						for _, errAckItem := range ackerr.(errors.ErrCode).Context()["Detail"].([]mq_http_sdk.ErrAckItem) {
							fmt.Printf("\tErrorHandle:%s, ErrorCode:%s, ErrorMsg:%s\n",
								errAckItem.ErrorHandle, errAckItem.ErrorCode, errAckItem.ErrorMsg)
						}
						time.Sleep(time.Duration(3) * time.Second)
					} else {
						fmt.Printf("Ack ---->\n\t%s\n", handles)
					}
					endChan <- 2

				}
			case err := <-errChan:
				{
					// 没有消息
					if strings.Contains(err.(errors.ErrCode).Error(), "MessageNotExist") {
					} else {
						fmt.Println(err)
						time.Sleep(time.Duration(3) * time.Second)
					}
					endChan <- 1
				}
			case <-time.After(10 * time.Second):
				{
					fmt.Println("Timeout of consumer message ??")
					endChan <- 1
				}
			}
		}()

		// 长轮询消费消息
		// 长轮询表示如果topic没有消息则请求会在服务端挂住3s，3s内如果有消息可以消费则立即返回
		mqConsumer.ConsumeMessage(respChan, errChan,
			16, // 一次最多消费10条(最多可设置为16条)
			0,  // 长轮询时间3秒（最多可设置为30秒）
		)
		endChanRes := <-endChan
		if endChanRes == 2 {
			goto RES
		} else {
			goto RESFALSE
			//continue
		}
	}
RES:
	return true, exchange
RESFALSE:
	return false, exchange
}
