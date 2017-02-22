PubSum A google Pub/Sub aggregator

Listening to several topics and redirect the data to one consolidating channel or topics.

Features:
-------------
Take a topic name pattern.

Create a listener for each topic.

Clean up.

Refresh in intervals


Roadmap:
---------
Adding and Removing topics explicetly from the library caller

Redirect to a topic in Google Pub/Sub

Redirect to a topic in Amazon SqS

Usage:
----------
```golang
func TestExample(t *testing.T) {
	project := `MyProject`
	//Create a context
	context := Context{
		Project:             project,
		RefreshInterval:     time.Hour * 1,
		SubscriptionPostfix: `-aggrigator`,
		TopicsFilter: func(topicName string) bool {
			return strings.HasPrefix(topicName, fmt.Sprintf("projects/%s/topics/myFormat-", project))
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
```
