package common

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConsume(t *testing.T) {
	e, resData := ConsumeContent("topic")
	if e == true && len(resData) > 0 {
		for _, item := range resData {
			fmt.Println(item)
		}
	}else {
		fmt.Println("ConsumeContent error")
	}
}


func TestMQProductAll(t *testing.T)  {
	var info []byte
	/*switch data.Mq_type {
	case 1:
		info, _ = json.Marshal(data.Mq_dyn_log)
	}*/
	data := map[string]interface{}{
		"test":"1",
	}
	info, _ = json.Marshal(data)
	MQProduct(string(info), Topic_sandan)
}
