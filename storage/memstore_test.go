/*
 * Copyright (c) 2020. Victor Ruscitto (vrus@vrcyber.com). All rights reserved.
 */

package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestCreation(t *testing.T) {
	t.Parallel()

	setMemStoreEnv()
	/*
		ms, err := NewMemstore("localhost", "6379", "")

		if err != nil {
			log.Printf("Failed to create memorystore. %v", err)
		}
		userAuth := &auth.User{
			ID:    123,
			UID:   "xakuhhkerwer",
			Email: "vrus@vrcyber.com",
		}

		if ok := ms.SaveKey("1", userAuth, 0); !ok {
			fmt.Printf("failed to save key")
		} else {
			var new auth.User

			if err := ms.GetKey("1", &new); err != nil {
				fmt.Printf("failed to get key")
			} else {
				fmt.Printf("Key Value: %+v ", new)
			}
		}

		stringVal := "1jhafkjbkdfhsdf"

		if ok := ms.SaveKey("2", stringVal, 0); !ok {
			fmt.Printf("failed to save key")
		} else {
			var new string

			if err := ms.GetKey("2", &new); err != nil {
				fmt.Printf("failed to get key")
			} else {
				fmt.Printf("Key Value: %+v ", new)
			}
		}
	*/
	unsetMemStoreEnv()
}

func TestSets(t *testing.T) {
	t.Parallel()

	setMemStoreEnv()
	ms, err := NewMemstore("localhost", "6379", "")

	if err != nil {
		println("Failed to create memorystore. %v", err)
	}

	key := "orgs:2"
	v := 2
	for i := 0; i < 5; i++ {
		if ok := ms.AddIntToSet(key, v); !ok {
			println("failed to add")
			return
		}
		v++
	}

	if vals, ok := ms.GetIntSet(key); !ok {
		println("failed to get")
		return
	} else {
		println(fmt.Sprintf("vals: %v", vals))
	}

	unsetMemStoreEnv()
}

func setMemStoreEnv() {
	os.Setenv("REDIS_HOST", "127.0.0.1")
	os.Setenv("REDIS_PORT", "6379")
}

func unsetMemStoreEnv() {
	os.Unsetenv("REDIS_HOST")
	os.Unsetenv("REDIS_PORT")
}
