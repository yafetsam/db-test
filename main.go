package main

import (
	"context"
	"database/sql"
	"db-test/internal/db"
	_ "github.com/lib/pq"

	"fmt"
	"log"
	"time"
)

const (
	mainConnection    = "host=localhost port=5432 user=postgres password=postgres dbname=testDB sslmode=disable"
	replicaConnection = "host=localhost port=5433 user=postgres password=postgres dbname=testDB sslmode=disable"
)

func main() {
	mainDB, err := sql.Open("postgres", mainConnection)
	if err != nil {
		log.Fatal("failed to connect to main db: ", err)
	}
	defer mainDB.Close()

	mainQueries := db.New(mainDB)
	// Insert a new order to master
	order, err := mainQueries.InsertOrder(context.TODO(), db.InsertOrderParams{
		ProductName: "test product",
		Quantity:    6,
		OrderDate:   time.Now(),
	})

	if err != nil {
		log.Fatal("Insert failed: ", err)
	}

	fmt.Println("Insert to main db successfull (should reflect in replica)", order)

	replicaDB, err := sql.Open("postgres", replicaConnection)
	if err != nil {
		log.Fatal("failed to connect to replica db: ", err)
	}

	replicaQueries := db.New(replicaDB)

	// it might take some time to sync
	time.Sleep(3 * time.Second)

	orders, err := mainQueries.ListOrder(context.TODO())
	if err != nil {
		log.Fatal("failed to connect to main db: ", err)
	}

	fmt.Printf("orders in main: ")
	for _, order := range orders {
		fmt.Printf("ID %d, Product Name: %s, Quantity: %d, Date: %s\n", order.ID, order.ProductName, order.Quantity, order.OrderDate.Format("2025-01-02"))
	}

	orders, err = replicaQueries.ListOrder(context.TODO())
	if err != nil {
		log.Fatal("failed to connect to replica db: ", err)
	}

	fmt.Printf("orders in replica: ")
	for _, order := range orders {
		fmt.Printf("ID %d, Product Name: %s, Quantity: %d, Date: %s", order.ID, order.ProductName, order.Quantity, order.OrderDate.Format("2025-01-02"))
	}
}
