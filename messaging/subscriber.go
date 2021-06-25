/*
 * Copyright (c) 2020. Victor Ruscitto (vrus@vrcyber.com). All rights reserved.
 */

package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
)

// Subscriber exposes the functionality behind the Google Pub/Sub
type Subscriber struct {
	client *pubsub.Client
	sub    *pubsub.Subscription
	stop   bool
}

// NewSubscriber creates a new Subscriber Interface in Pull configuration
func NewSubscriber(projectID string, topic string) (*Subscriber, error) {
	// Initialize Pub/Sub Client
	client, err := pubsub.NewClient(context.Background(), projectID)

	if err != nil {
		return nil, err
	}

	sub := client.Subscription(topic)
	sub.ReceiveSettings.Synchronous = false
	// number of goroutines sub.Receive will spawn to pull messages concurrently.
	//sub.ReceiveSettings.NumGoroutines = runtime.NumCPU()

	// This is only guaranteed when ReceiveSettings.Synchronous is set to true.
	// When Synchronous is set to false, the StreamingPull RPC is used which
	// can pull a single large batch of messages at once that is greater than
	// MaxOutstandingMessages before pausing. For more info, see
	// https://cloud.google.com/pubsub/docs/pull#streamingpull_dealing_with_large_backlogs_of_small_messages.
	//sub.ReceiveSettings.MaxOutstandingMessages = 20

	return &Subscriber{
		client: client,
		sub:    sub,
		stop:   false,
	}, nil
}

// Start begins the receive cycle of messages. f will receive the callback with the message details to process
func (s *Subscriber) Start(ctx context.Context, f func(ctx context.Context, eventType string, msg map[string]interface{}) bool) error {
	var mu sync.Mutex
	cctx, cancel := context.WithCancel(ctx)
	err := s.sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		//	Only use the Mutex if we use Synchronous = false above
		mu.Lock()
		defer mu.Unlock()

		//fmt.Printf("Got message: %q\n", string(msg.Data))
		var data map[string]interface{}
		err := json.Unmarshal(msg.Data, &data)

		if err == nil {
			eventType := msg.Attributes["eventType"]
			//fmt.Printf("Received message event: %s\n", eventType)

			// This will call back to our processing function and if we get back a valid response
			// we will Ack the message
			if f(ctx, eventType, data) {
				msg.Ack()
			}
		}

		if s.stop {
			cancel()
			fmt.Printf("Subscriber: Cancel Requested.")
		}
	})
	if err != nil {
		return err
	}

	return nil
}

// Stop signals the Receive function to cancel listening
func (s *Subscriber) Stop() {
	s.stop = true
}
