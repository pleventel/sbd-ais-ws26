# Exercise 3 &ndash; Solutions

## Running files:
Go to this `/solution` folder and run the following commads:
~~~bash
# 1. install the OpenAPI generator
go install github.com/swaggo/swag/cmd/swag@v1.16.6

# 2. generate the OpenAPI docs
cd scripts
sh build-openapi-docs.sh
cd ..

# 3. start the server
go run .
~~~


## What was implemented?
### `/model`:
Data definition. Editied files: `drink.go` and `order.go`.

Here the missing parameters (so everything other than the ID) have been defined.

### `/repository`:
The in-memory database. Edited file: `db.go`.

Created a demo database (`NewDatabaseHandler()`) with test data, added a function for order totals (`GetTotalledOrders()`). Adding a new order is also possible with `AddOrder()`.

### `/rest`:
The handlers. Edited file: `api.go`.

Created the functions `GetMenu`, `GetOrders`, `GetOrdersTotal` to read from the database and the orders can be posted/decoded (`PostOrder`).