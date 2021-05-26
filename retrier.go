package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go"
	"go.mongodb.org/mongo-driver/mongo"
)

const numOfRetries = 1000000

type retrier struct {
	count int32
}

func (r *retrier) incCount() {
	atomic.AddInt32(&r.count, 1)
}

func (r *retrier) getCount() int {
	return int(atomic.LoadInt32(&r.count))
}

func (r *retrier) retryOnRequestTooHigh(f func() error) error {
	return retry.Do(func() error {
		err := f()
		if err == nil {
			return nil
		}
		if isRequestTooHighErr(err) {
			r.incCount()
			fmt.Println("retried")
			return err
		}
		return retry.Unrecoverable(err)
	}, retry.Attempts(numOfRetries), retry.LastErrorOnly(true), retry.MaxDelay(time.Second))
}

const requestRateTooHighCode = 16500

func isRequestTooHighErr(err error) bool {
	if ce, ok := err.(mongo.CommandError); ok {
		return ce.Code == requestRateTooHighCode
	}

	if bwe, ok := err.(mongo.WriteException); ok {
		for _, we := range bwe.WriteErrors {
			if we.Code == requestRateTooHighCode {
				return true
			}
		}
	}

	if bwe, ok := err.(mongo.BulkWriteException); ok {
		for _, we := range bwe.WriteErrors {
			if we.Code == requestRateTooHighCode {
				return true
			}
		}
	}

	return false
}
