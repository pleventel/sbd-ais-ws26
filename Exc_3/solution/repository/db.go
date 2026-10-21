package repository

import (
	"ordersystem/model"
	"sync"
	"time"
)

type DatabaseHandler struct {
	// drinks represent all available drinks
	drinks []model.Drink
	// orders serves as order history
	orders []model.Order

	// every HTTP request runs in its own goroutie.
	mu sync.RWMutex
}

// todo
func NewDatabaseHandler() *DatabaseHandler {
	// Init the drinks slice with some test data
	// drinks := ...
	drinks := []model.Drink{
		{ID: 1, Name: "Dark Beer",     Price: 3.60, Description: "Cold; 0.5 l."},
		{ID: 2, Name: "Light Beer",    Price: 3.20, Description: "Alcohol free, cold, 0.5 l."},
		{ID: 3, Name: "Coke",          Price: 2.80, Description: "Cold, 0.33 l."},
		{ID: 4, Name: "Mineral Water", Price: 2.00, Description: "0.5 l."},
		{ID: 5, Name: "Tap Water",     Price: 1.00, Description: "biggest lie ever..."},
	}

	// Init orders slice with some test data
	now := time.Now()
	orders := []model.Order{
		{DrinkID: 1, Amount: 2, CreatedAt: now.Add(-30 * time.Minute)},
		{DrinkID: 3, Amount: 1, CreatedAt: now.Add(-25 * time.Minute)},
		{DrinkID: 2, Amount: 3, CreatedAt: now.Add(-20 * time.Minute)},
		{DrinkID: 5, Amount: 2, CreatedAt: now.Add(-12 * time.Minute)},
		{DrinkID: 1, Amount: 4, CreatedAt: now.Add(-8 * time.Minute)},
		{DrinkID: 4, Amount: 1, CreatedAt: now.Add(-3 * time.Minute)},
	}

	return &DatabaseHandler{
		drinks: drinks,
		orders: orders,
	}
}

func (db *DatabaseHandler) GetDrinks() []model.Drink {
	return db.drinks
}

func (db *DatabaseHandler) GetOrders() []model.Order {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Return a copy of the orders
	orders := make([]model.Order, len(db.orders))
	copy(orders, db.orders)
	return db.orders
}

// todo
func (db *DatabaseHandler) GetTotalledOrders() map[uint64]uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// calculate total orders
	// key = DrinkID, value = Amount of orders
	totalledOrders := make(map[uint64]uint64)
	for _, order := range db.orders {
		totalledOrders[order.DrinkID] += order.Amount
	}
	return totalledOrders
}

func (db *DatabaseHandler) AddOrder(order *model.Order) {
	// todo
	// add order to db.orders slice
	if order != nil {
		db.orders = append(db.orders, *order)
	}
}
