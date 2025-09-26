// Copyright 2025 Seth Itow
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ObjectStorageClient provides a way to mock this out in tests
type ObjectStorageClient interface {
	PutObject(ctx context.Context, key string, data []byte) (string, error)
	GetObject(ctx context.Context, key string) ([]byte, error)
	GetObjectRange(ctx context.Context, key string, start, end int64) ([]byte, error)
	DeleteObject(ctx context.Context, key string) error
	DeleteObjects(ctx context.Context, prefix string) error
	ListObjects(ctx context.Context, prefix string) ([]StoredObject, error)
	HeadObject(ctx context.Context, key string) (*StoredObject, error)
}

type StoredObject struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}

// objectStorageClient implements the ObjectStoreClient interface
type objectStorageClient struct {
	b string // bucket
	p string // prefix
	c *minio.Client
}

func newObjectStorageClient(oscfg ObjectStoreConfig) (ObjectStorageClient, error) {
	if oscfg.Bucket == "" {
		return nil, fmt.Errorf("object storage bucket name is required")
	}

	endpoint := oscfg.Endpoint
	if endpoint == "" {
		endpoint = "https://s3.amazonaws.com"
	}

	region := oscfg.Region
	if region == "" {
		region = "us-east-1"
	}

	c, err := minio.New(oscfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(oscfg.AccessKeyID, oscfg.SecretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	client := &objectStorageClient{
		c: c,
		b: oscfg.Bucket,
		p: oscfg.PathPrefix,
	}

	// TODO: bucket healthcheck

	return client, nil
}

func (c *objectStorageClient) PutObject(ctx context.Context, key string, data []byte) (string, error) {
	i, err := c.c.PutObject(ctx, c.b, path.Join(c.p, key), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return "", err
	}
	return i.ETag, nil
}

func (c *objectStorageClient) GetObject(ctx context.Context, key string) ([]byte, error) {
	obj, err := c.c.GetObject(ctx, c.b, path.Join(c.p, key), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return io.ReadAll(obj)
}

// GetObjectRange downloads a specific range of bytes
func (c *objectStorageClient) GetObjectRange(ctx context.Context, key string, start, end int64) ([]byte, error) {
	var opts minio.GetObjectOptions
	opts.SetRange(start, end)
	obj, err := c.c.GetObject(ctx, c.b, path.Join(c.p, key), opts)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(obj)
}

func (c *objectStorageClient) DeleteObject(ctx context.Context, key string) error {
	return c.c.RemoveObject(ctx, c.b, path.Join(c.p, key), minio.RemoveObjectOptions{})
}

// ListObjects lists objects with the given prefix
// "/" is appended to the prefix so it matches a full "directory"
func (c *objectStorageClient) ListObjects(ctx context.Context, prefix string) ([]StoredObject, error) {
	objChan := c.c.ListObjects(ctx, c.b, minio.ListObjectsOptions{Prefix: path.Join(c.p, prefix) + "/"})

	objects := make([]StoredObject, 0)

	for object := range objChan {
		objects = append(objects, StoredObject{
			Key:          strings.TrimPrefix(object.Key, c.p+"/"),
			Size:         object.Size,
			LastModified: object.LastModified,
			ETag:         object.ETag,
		})
	}

	return objects, nil
}

func (c *objectStorageClient) DeleteObjects(ctx context.Context, prefix string) error {
	objCh := c.c.ListObjects(ctx, c.b, minio.ListObjectsOptions{Prefix: path.Join(c.p, prefix) + "/", Recursive: true})
	errCh := c.c.RemoveObjects(ctx, c.b, objCh, minio.RemoveObjectsOptions{})
	var errs []error
	for e := range errCh {
		errs = append(errs, &e)
	}
	return errors.Join(errs...)
}

// HeadObject gets metadata about an object without downloading it
func (c *objectStorageClient) HeadObject(ctx context.Context, key string) (*StoredObject, error) {
	obj, err := c.c.StatObject(ctx, c.b, path.Join(c.p, key), minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}

	return &StoredObject{
		Key:          key,
		Size:         obj.Size,
		LastModified: obj.LastModified,
		ETag:         obj.ETag,
	}, nil
}

// isObjNotFound checks if an error represents a "not found" condition
func isObjNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrStoreMsgNotFound) {
		return true
	}
	if minioErr, ok := err.(minio.ErrorResponse); ok {
		if minioErr.Code == minio.NoSuchKey {
			return true
		}
	}
	return false
}
