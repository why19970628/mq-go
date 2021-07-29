package common

import "testing"

type MQTestData struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func (d *MQTestData) MQTag() string {
	return "test_tag"
}

func (d *MQTestData) MQTopic() string {
	return "test_topic"
}

func TestMQProduce(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := Produce(&MQTestData{
			Name:  "测试数据",
			Count: i,
		})
		if err != nil {
			t.Error(err)
		}
	}
}
