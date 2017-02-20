package pubsum

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"
)

func TestExample(t *testing.T) {
	project := `infra-01`
	//Create a context
	context := Context{
		Project:             project,
		RefreshInterval:     time.Hour * 1,
		SubscriptionPostfix: `-monitor`,
		TopicsFilter: func(topicName string) bool {
			return strings.HasPrefix(topicName, fmt.Sprintf("projects/%s/topics/adsmanager-", project))
		},
	}

	//Get output channel
	receive := New(context).Start()

	//Read all messages from the consolidation channel
	for message := range receive {
		//ACK the message
		message.Done(true)
		log.Printf("in test %+v\n", string(message.Data))
	}

	//aggregator.Stop(true)
}

func TestUpdate(t *testing.T) {
	project := `MyProject`
	//Create a context
	context := Context{
		Project:             project,
		RefreshInterval:     time.Hour * 1,
		SubscriptionPostfix: `-aggrigator`,
		TopicsFilter: func(topicName string) bool {
			return strings.HasPrefix(topicName, fmt.Sprintf("projects/%s/topics/myPrefixFormat-", project))
		},
	}
	list := []string{
		`projects/MyProject/topics/customer-5222`,
		`projects/MyProject/topics/customer-5111`,
	}
	aggrigator := New(context)
	current := make(topicsMap)
	newContainer, _ := aggrigator.update(current, list)

	if len(newContainer) != 2 {
		t.Log(len(newContainer))
		t.Fail()
	}
}
