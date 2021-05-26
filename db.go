package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type dbType int

const (
	mongoDB dbType = iota + 1
	cosmosDB
)

type db struct {
	col     *mongo.Collection
	retrier *retrier
}

func newDB(ctx context.Context, dbt dbType) (*db, error) {
	opts := &options.ClientOptions{}
	dbclient := &db{}

	switch dbt {
	case mongoDB:
		opts.ApplyURI("")
	case cosmosDB:
		opts.ApplyURI("")
		dbclient.retrier = &retrier{}
	default:
		return nil, fmt.Errorf("invalid db type")
	}

	c, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err := c.Ping(ctx, nil); err != nil {
		return nil, err
	}

	dbclient.col = c.Database("yshnitsar-test").Collection("test")
	return dbclient, nil
}

func (d *db) insert(ctx context.Context, docs []interface{}) error {
	return d.retry(func() error {
		_, err := d.col.InsertMany(ctx, docs)
		return err
	})
}

func (d *db) transactionalInsert(ctx context.Context, docs []interface{}) error {
	return d.col.Database().Client().UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err := sessionContext.StartTransaction(); err != nil {
			return err
		}

		if err := d.insert(ctx, docs); err != nil {
			if err := sessionContext.AbortTransaction(ctx); err != nil {
				return fmt.Errorf("failed to abort transaction: %w", err)
			}
			return err
		}

		return sessionContext.CommitTransaction(ctx)
	})
}

func (d *db) totalSumByStatus(ctx context.Context) error {
	type resp struct {
		ID    string  `bson:"_id"`
		Total float64 `bson:"total"`
	}

	return d.retry(func() error {
		sum := bson.D{{"$group", bson.D{{"_id", "$status"}, {"total", bson.D{{"$sum", "$total"}}}}}}

		opts := options.Aggregate().SetMaxTime(time.Minute * 20)
		cur, err := d.col.Aggregate(ctx, mongo.Pipeline{sum}, opts)
		if err != nil {
			return err
		}

		var totalByStatus []resp
		if err = cur.All(ctx, &totalByStatus); err != nil {
			return err
		}
		// todo: check correctness with cosmos
		//fmt.Println(totalByStatus)
		return nil
	})
}

func (d *db) findByID(ctx context.Context, id primitive.ObjectID) error {
	var ts testStruct
	err := d.retry(func() error {
		sr := d.col.FindOne(ctx, bson.M{"_id": id})
		return sr.Decode(&ts)
	})
	if err != nil {
		return err
	}

	// check the the validity of the record
	if ts.CreatedAt.IsZero() {
		return errors.New("invalid record")
	}
	return nil
}

func (d *db) findMany(ctx context.Context, size int64) ([]testStruct, error) {
	opts := options.Find()
	opts.Limit = &size

	var ts []testStruct
	err := d.retry(func() error {
		cur, err := d.col.Find(ctx, bson.M{}, opts)
		if err != nil {
			return err
		}
		return cur.All(ctx, &ts)
	})
	if err != nil {
		return nil, err
	}

	return ts, nil
}

func (d *db) retry(f func() error) error {
	if d.retrier == nil {
		return f()
	}

	return d.retrier.retryOnRequestTooHigh(f)
}
