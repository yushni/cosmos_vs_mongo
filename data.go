package main

import (
	"math/rand"
	"time"

	"github.com/tjarratt/babble"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var (
	wordGenerator = babble.NewBabbler()
	statuses      = []string{"alive", "effective", "operating", "flowing", "functioning", "going", "mobile", "movable",
		"operative", "progressive", "pushing", "rapid", "rolling", "running", "rushing", "rustling", "shifting",
		"simmering", "speeding", "streaming", "traveling", "turning", "walking", "working", "astir", "at work",
		"bustling", "efficacious", "exertive", "hasty", "impelling", "in force", "in play", "in process", "moving",
		"restless", "speedy", "swarming",
	}
)

type testStruct struct {
	ID          primitive.ObjectID `bson:"_id"`
	Title       string
	Description string
	Status      string
	Total       int
	CreatedAt   time.Time
}

func generateTestStructs(n int) []interface{} {
	resp := make([]interface{}, n)

	for i := range resp {
		resp[i] = testStruct{
			ID:          primitive.NewObjectID(),
			Title:       wordGenerator.Babble(),
			Description: wordGenerator.Babble(),
			Status:      statuses[rand.Intn(len(statuses)-1)],
			Total:       rand.Intn(55),
			CreatedAt:   time.Now(),
		}
	}

	return resp
}
