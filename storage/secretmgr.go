/*
 * Copyright (c) 2020. Victor Ruscitto (vrus@vrcyber.com). All rights reserved.
 */

package storage

import (
	"context"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

type SecretMgr struct {
	client      *secretmanager.Client
	projectPath string
}

// NewSecretMgr
func NewSecretMgr(projectPath string) (*SecretMgr, error) {
	// ProjectPath looks like this: projects/123456789/secrets
	client, err := secretmanager.NewClient(context.Background())

	if err != nil {
		return nil, fmt.Errorf("failed to create SecretManager client: %v", err)
	}

	return &SecretMgr{
		client:      client,
		projectPath: projectPath,
	}, nil
}

// GetSecret
func (s *SecretMgr) GetSecret(name string) (string, error) {
	fullPath := fmt.Sprintf("%s/%s/versions/latest", s.projectPath, name)

	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fullPath,
	}

	result, err := s.client.AccessSecretVersion(context.Background(), req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %v", err)
	}

	return string(result.Payload.Data), nil
}
