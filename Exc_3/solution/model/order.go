package model

import "time"

type Order struct {
	DrinkID   uint64    `json:"drink_id"` // foreign key
	CreatedAt time.Time `json:"createdat"`
	Amount    uint64     `json:"amount"`
}
