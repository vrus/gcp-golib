/*
 * Copyright (c) 2020. Victor Ruscitto (vrus@vrcyber.com). All rights reserved.
 */

package storage

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"cloud.google.com/go/storage"
)

type FileStore struct {
	client *storage.Client
}

// NewFileStore
func NewFileStore() (*FileStore, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	return &FileStore{
		client: client,
	}, nil
}

// UploadFile
func (f *FileStore) UploadFile(bucket string, filename string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	// Upload an object with storage.Writer.
	wc := f.client.Bucket(bucket).Object(filename).NewWriter(ctx)
	wc.Write(data)

	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}
	return nil
}

// DownloadFile
func (f *FileStore) DownloadFile(bucket string, filename string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	rc, err := f.client.Bucket(bucket).Object(filename).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %v", filename, err)
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll: %v", err)
	}
	return data, nil
}

// DeleteFile
func (f *FileStore) DeleteFile(bucket string, filename string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	if err := f.client.Bucket(bucket).Object(filename).Delete(ctx); err != nil {
		return fmt.Errorf("Object(%q).Delete: %v", filename, err)
	}

	return nil
}

// Close
func (f *FileStore) Close() {
	f.client.Close()
}
