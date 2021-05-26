package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

const (
	total            = 100
	parallelRequests = 25
	docInBatch       = 1

	dbInUse = cosmosDB
)

var initTestCase = aggregationTC

func main() {
	start := time.Now()
	ctx := context.Background()

	db, err := newDB(ctx, dbInUse)
	if err != nil {
		panic(err)
	}

	r := runner{total: total, parallelRequests: parallelRequests, priceOfIteration: docInBatch}
	if err = r.run(initTestCase(ctx, db)); err != nil {
		panic(err)
	}

	retries := 0
	if db.retrier != nil {
		retries = db.retrier.getCount()
	}
	fmt.Println("finished in ", time.Since(start), "retried ", retries)
}

/**
 TEST CASES
**/
func insertTC(ctx context.Context, db *db) func() error {
	return func() error {
		return db.insert(ctx, generateTestStructs(docInBatch))
	}
}

func transactionalInsertTC(ctx context.Context, db *db) func() error {
	return func() error {
		return db.transactionalInsert(ctx, generateTestStructs(docInBatch))
	}
}

func aggregationTC(ctx context.Context, db *db) func() error {
	return func() error {
		return db.totalSumByStatus(ctx)
	}
}

func findTC(ctx context.Context, db *db) func() error {
	loadSize := int64(total / 10)
	testSearch, err := db.findMany(ctx, loadSize)
	if err != nil {
		return func() error { return fmt.Errorf("failed to find documents: %w", err) }
	}

	// it should give some time for database to recover resources
	fmt.Printf("sleep 10 seconds after load of %d docs\n", loadSize)
	time.Sleep(time.Second * 10)

	return func() error {
		return db.findByID(ctx, testSearch[rand.Int63n(loadSize-1)].ID)
	}
}
