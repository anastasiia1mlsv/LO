package main

import (
	sd "LO_WB/SharedData"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
)

var (
	// вынести в env.file переменные окружения
	// есть библиотека
	HOST string = "localhost"
	PORT int    = 5432
	USER string = "postgres"
	PASS string = "postgres"
	NAME string = "postgres"
)

func TableCreateIfNotExists() error {
	query := `CREATE TABLE IF NOT EXISTS public.orders_json
	(
		id text NOT NULL,
		data text,
		PRIMARY KEY (id)
	);

	ALTER TABLE IF EXISTS public.orders_json
	OWNER to postgres;`

	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", HOST, PORT, USER, PASS, NAME)

	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return err
	}

	defer db.Close()

	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func TableInsertOrder(key string, data string) error {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", HOST, PORT, USER, PASS, NAME)

	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return err
	}

	defer db.Close()

	insertDynStmt := `insert into "orders_json"("id", "data") values($1, $2)`
	_, err = db.Exec(insertDynStmt, key, data)
	if err != nil {
		return err
	}
	return nil
}

type Record struct {
	Key  string
	Data string
}

func TableSelectOrders() error {

	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", HOST, PORT, USER, PASS, NAME)

	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return err
	}

	defer db.Close()

	rows, err := db.Query("select * from \"orders_json\"")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		r := Record{}
		err := rows.Scan(&r.Key, &r.Data)
		if err != nil {
			fmt.Println(err)
			continue
		}
		var order sd.Order = sd.Order{} // литерал
		err = json.Unmarshal([]byte(r.Data), &order)
		if err != nil {
			log.Println("Invalid JSON", err)
			continue
		} // ссылка на ордер
		InternalStorageSet(&order, r.Key) // не сам объект а копия
	}
	return nil
}
