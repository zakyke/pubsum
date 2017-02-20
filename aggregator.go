package pubsum

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/pubsub"
)

type subscriber struct {
	sub     *pubsub.Subscription
	ch      chan *struct{}
	version int64
}

type topicsMap map[string]*subscriber

type atomicTopicMap struct {
	*atomic.Value
}

//Aggregator the service
type Aggregator struct {
	Context
	messages chan *pubsub.Message
	atom     atomicTopicMap
}

//Context the service context
type Context struct {
	Project             string
	RefreshInterval     time.Duration
	SubscriptionPostfix string
	TopicsFilter        func(topicName string) bool
}

//New create new service
func New(ctx Context) *Aggregator {
	atom := atomicTopicMap{}
	atom.Value = &atomic.Value{}
	atom.Store(make(topicsMap))
	return &Aggregator{Context: ctx, atom: atom, messages: make(chan *pubsub.Message)}
}

//Start the service workers
func (agg *Aggregator) Start() chan *pubsub.Message {
	go agg.updaterWorker()
	return agg.messages
}

//Stop the service workers
//If deleteSubscription true all subscription will be deleted
func (agg *Aggregator) Stop(deleteSubscription bool) {
	mp := agg.atom.Load().(topicsMap)
	for k := range mp {
		if deleteSubscription {
			if err := mp[k].sub.Delete(context.Background()); err != nil {
				log.Println(`subscription deletion failed `, mp[k].sub.Name(), ` `, err)
			}
		}
		close(mp[k].ch)
	}
}

func (agg *Aggregator) updaterWorker() {
	ticker := time.NewTicker(agg.RefreshInterval)
	do := func() {
		if newTopicsList, err := agg.fetchTopics(); err == nil {
			current := agg.atom.Load().(topicsMap)
			if updatedTopicsMap, err := agg.update(current, newTopicsList); err == nil {
				agg.atom.Store(updatedTopicsMap)
			}
		}
	}

	do()

	for range ticker.C {
		do()
	}
}

func (agg *Aggregator) update(current topicsMap, newTopicsName []string) (topicsMap, error) {
	log.Println(`newTopicsName `, newTopicsName)
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, agg.Project)
	if err != nil {
		log.Printf("could not create pubsub Client: %v", err)
		return nil, err
	}

	createSubscriber := func(topicName string) (*pubsub.Subscription, error) {
		tp := client.Topic(topicName)
		subName := topicName + agg.SubscriptionPostfix
		sub, err := client.CreateSubscription(ctx, subName, tp, 600*time.Second, nil)
		if err != nil {
			//The error may be googleapi: Error 409: Resource already exists in the project.
			//In this case try to get the subscription
			log.Println(`error creating subscription `, err)
			sub = client.Subscription(subName)
			if sub != nil {
				return sub, nil
			}
			return nil, fmt.Errorf("fail to get or create subscription %s", err)
		}
		return sub, nil
	}

	version := time.Now().Unix()
	pos := 0
	for _, v := range newTopicsName {
		if _, ok := current[v]; ok {
			current[v].version = version
			continue
		}
		pos = strings.Index(v, `/topics/`)
		if newsub, err := createSubscriber(v[pos+len(`/topics/`):]); err == nil {
			ch := make(chan *struct{})
			current[v] = &subscriber{ch: ch, version: version, sub: newsub}
			go subscriberWorker(newsub, ch, agg.messages)
			ch <- nil
		}
	}

	for k := range current {
		if current[k].version != version {
			//Closing the channel will exit the subscription goroutine worker
			if err := current[k].sub.Delete(context.Background()); err != nil {
				log.Println(`subscription deletion failed `, current[k].sub.Name(), ` `, err)
			}
			close(current[k].ch) // Finishing the goroutine
			delete(current, k)
		}
	}
	return current, nil
}

func subscriberWorker(sub *pubsub.Subscription, in chan *struct{}, out chan *pubsub.Message) {
	defer log.Println(`stop listinig to `, sub.Name())
	it, err := sub.Pull(context.Background())
	if err != nil {
		log.Println(`error creating pull `, err)
		return
	}
	for range in {
		log.Println(`start listinig to `, sub.Name())
		for {
			msg, err := it.Next()
			if err != nil {
				log.Println(`could not pull: `, err)
				continue
			}
			out <- msg
		}
	}
}

func (agg *Aggregator) fetchTopics() ([]string, error) {
	log.Println(`start fetching topicsMap`)
	defer log.Println(`finish fetching topicsMap`)
	var topics []string
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, agg.Project)
	if err != nil {
		log.Printf("could not create pubsub Client: %v", err)
		return nil, err
	}
	it := client.Topics(ctx)
	for {

		topic, err := it.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Println(err)
			return nil, err
		}

		if agg.TopicsFilter(topic.Name()) {
			topics = append(topics, topic.Name())
		}

	}
	return topics, nil
}
