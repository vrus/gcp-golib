/*
 * Copyright (c) 2020. Victor Ruscitto (vrus@vrcyber.com). All rights reserved.
 */

package messaging

import (
	"context"
	"fmt"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
)

type TaskMgr struct {
	client     *cloudtasks.Client
	ctx        context.Context
	projectID  string
	locationID string
}

// NewTaskMgr
func NewTaskMgr(projectID string, locationID string) (*TaskMgr, error) {
	ctx := context.Background()
	client, err := cloudtasks.NewClient(ctx)

	if err != nil {
		return nil, fmt.Errorf("NewTaskMgr: %v", err)
	}

	return &TaskMgr{
		client:     client,
		ctx:        ctx,
		projectID:  projectID,
		locationID: locationID,
	}, nil
}

// CreateTask submits tasks to AppEngine endpoints
func (t *TaskMgr) CreateTask(queueID string, data []byte, handler string) (*taskspb.Task, error) {
	headers := map[string]string{
		"Content-Type": "application/json",
	}

	// Build the Task payload.
	// https://godoc.org/google.golang.org/genproto/googleapis/cloud/tasks/v2#CreateTaskRequest
	req := &taskspb.CreateTaskRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s/queues/%s", t.projectID, t.locationID, queueID),
		Task: &taskspb.Task{
			// https://godoc.org/google.golang.org/genproto/googleapis/cloud/tasks/v2#AppEngineHttpRequest
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					HttpMethod:  taskspb.HttpMethod_POST,
					Headers:     headers,
					RelativeUri: handler,
				},
			},
		},
	}

	req.Task.GetAppEngineHttpRequest().Body = data
	task, err := t.client.CreateTask(t.ctx, req)

	if err != nil {
		return nil, fmt.Errorf("CreateTask: %v", err)
	}

	return task, nil
}

func (t *TaskMgr) CreateTaskHttp(queueID string, data []byte, handler string, serviceAccount string) (*taskspb.Task, error) {
	headers := map[string]string{
		"Content-Type": "application/json",
	}

	// Build the Task payload.
	// https://godoc.org/google.golang.org/genproto/googleapis/cloud/tasks/v2#CreateTaskRequest
	req := &taskspb.CreateTaskRequest{
		Parent: fmt.Sprintf("projects/%s/locations/%s/queues/%s", t.projectID, t.locationID, queueID),
		Task: &taskspb.Task{
			// https://godoc.org/google.golang.org/genproto/googleapis/cloud/tasks/v2#HttpRequest
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					HttpMethod: taskspb.HttpMethod_POST,
					Headers:    headers,
					Url:        handler,
					AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
						OidcToken: &taskspb.OidcToken{
							ServiceAccountEmail: serviceAccount,
						},
					},
				},
			},
		},
	}

	req.Task.GetHttpRequest().Body = data
	task, err := t.client.CreateTask(t.ctx, req)

	if err != nil {
		return nil, fmt.Errorf("CreateTask: %v", err)
	}

	return task, nil
}

// Close
func (t *TaskMgr) Close() {
	_ = t.client.Close()
}
