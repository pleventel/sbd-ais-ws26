package model

type Order struct {
	DrinkID   uint64    `json:"drink_id"` // foreign key
	CreatedAt time.Time `json:"createdat"`
	Amount    int64     `json:"amount"`
}
