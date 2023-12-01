package main

import (
	sd "LO_WB/SharedData"
	"encoding/json"
	"fmt"
	"github.com/labstack/echo/v4"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"log"
	"net/http"
	"sync"
	"time"
)

func init() {
	err := TableCreateIfNotExists()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Created DB successfully")

	err = TableSelectOrders()
	if err != nil {
		return
	}
	log.Println("Orders imported from DB")
	//os.Exit(1)
}

func main() {
	var err error
	var url string = "nats://localhost:4223"
	// Connect to a server
	var nc *nats.Conn
	nc, err = nats.Connect(url) //создаем объект nc типа nats connection
	if err != nil {
		log.Fatal(err) //2023/11/30 09:08:42 nats: no servers available for connection
	}
	fmt.Println("Connected to Nats")

	sc, err := stan.Connect("test-cluster", "test-client2", stan.NatsConn(nc),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			log.Fatalf("Connection lost, reason: %v", reason)
		}))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, url)
		return
	}

	fmt.Println("Connected to Nats Streaming")

	var topic string = "My-test-topic"
	// функция будет вызываться каждый раз когда придет сообщение
	// функция для подписчика
	var subscribeCallback = func(m *stan.Msg) {
		err := handleMessage(m)
		if err != nil {
			log.Println(err)
		}
	}
	sub, err := sc.Subscribe(topic, subscribeCallback, stan.DurableName("my-durable"))
	// durable name функция создает подписку с памятью у стэна
	// гарантирует доставку сообщений подписка с гарантированной доставкой
	if err != nil {
		log.Fatal(err)
	}

	defer sub.Unsubscribe() // defer функция не вызывается сразу, только после исполнения основной функции

	log.Println("Log HTTP starting...")

	e := echo.New()
	e.GET("/", func(c echo.Context) error {
		return c.HTML(http.StatusOK, "MY HOMEPAGE. SEE <a href=\"/orders\">ORDERS</a>")
	})

	e.GET("/orders", func(c echo.Context) error {
		var keys []string = InternalStorageKeys()
		return c.JSON(http.StatusOK, &keys) // ставим амперсант
	})

	e.GET("/orders/:id", func(c echo.Context) error {
		id := c.Param("id")
		return c.JSON(http.StatusOK, InternalStorageGet(id))
	})

	e.GET("/length", func(c echo.Context) error {
		var keys []string = InternalStorageKeys()
		return c.JSON(http.StatusOK, len(keys))
	})

	e.Logger.Fatal(e.Start(":8880"))
}

var InternalStorage map[string]*sd.Order = make(map[string]*sd.Order) // make инициализация маппы

func handleMessage(m *stan.Msg) error {
	log.Printf("Received message length: %d \n", len(m.Data))
	var order *sd.Order = &sd.Order{}
	err := json.Unmarshal(m.Data, order)
	if err != nil {
		return err
	}
	key := InternalStoragePut(order)
	err = TableInsertOrder(key, string(m.Data))
	if err != nil {
		return err
	}
	log.Println("Locale: ", order.Locale)
	return nil
}

var StorageMutex sync.Mutex // lock и unlock

func InternalStoragePut(order *sd.Order) string {
	var key string = KeyGenerator()
	// маппа должна быть защищена
	return InternalStorageSet(order, key)
}

func InternalStorageSet(order *sd.Order, key string) string {
	StorageMutex.Lock()
	defer StorageMutex.Unlock()
	InternalStorage[key] = order
	return key
}

func KeyGenerator() string {
	// используем метку времени как ключ и возвращаем стринг
	return fmt.Sprintf("%d", time.Now().UnixNano()) //
}

func InternalStorageGet(key string) *sd.Order {
	StorageMutex.Lock()
	defer StorageMutex.Unlock()

	return InternalStorage[key]
}

func InternalStorageKeys() []string {
	StorageMutex.Lock()
	defer StorageMutex.Unlock()
	var keys []string = make([]string, len(InternalStorage)) //создает срез строк такая же как маппа такой же длины
	var i int = 0
	for k := range InternalStorage {
		keys[i] = k
		i++
	}

	return keys
}
