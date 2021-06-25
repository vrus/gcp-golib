/*
 * Copyright (c) 2020. Victor Ruscitto (vrus@vrcyber.com). All rights reserved.
 */

package messaging

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
)

type Publisher struct {
	client *pubsub.Client
	topics map[string]*pubsub.Topic
	ctx    context.Context
}

// NewPublisher creates a Publisher client. It will setup topics based on all the topic names that are passed in.
func NewPublisher(projectID string, topics []string) (*Publisher, error) {
	//ctx, _ = context.WithTimeout(context.Background(), 10*time.Minute)
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)

	if err != nil {
		return nil, err
	}

	t := make(map[string]*pubsub.Topic)

	for _, name := range topics {
		// loop through all the requested topic names and make sure they are valid
		topic := client.Topic(name)
		exists, err := topic.Exists(ctx)

		if err != nil || !exists {
			return nil, fmt.Errorf("couldn't find topic %v. %v", name, err)
		}

		t[name] = topic
	}

	return &Publisher{
		client: client,
		topics: t,
		ctx:    ctx,
	}, nil
}

// PublishMessage will take a set of bytes and publish the message to the specified topic
func (s *Publisher) PublishMessage(topic string, data []byte) error {
	// Fetch the PubSub Topic pointer from the map
	if t, ok := s.topics[topic]; ok {
		res := t.Publish(s.ctx, &pubsub.Message{
			Data: data,
		})

		if _, err := res.Get(s.ctx); err != nil {
			return fmt.Errorf("publish result: %v", err)
		}

		return nil
	}

	return fmt.Errorf("invalid Topic specified: %v", topic)
}
