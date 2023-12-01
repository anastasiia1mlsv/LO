package main

import (
	sd "LO_WB/SharedData"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"log"
	"time"
)

func main() {
	var err error
	var url string = "nats://localhost:4223"
	var nc *nats.Conn
	nc, err = nats.Connect(url)
	if err != nil {
		log.Fatal(err) //2023/11/30 09:08:42 nats: no servers available for connection
	}
	fmt.Println("CONNECTED TO NATS")

	sc, err := stan.Connect("test-cluster", "test-client1", stan.NatsConn(nc),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			log.Fatalf("Connection lost, reason: %v", reason)
		}))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, url)
		return
	}

	fmt.Println("CONNECTED TO NATS STREAMING")

	var topic string = "My-test-topic"
	// функция будет вызываться каждый раз когда придет сообщение
	// функция для подписчика
	var subscribeCallback = func(m *stan.Msg) {
		//log.Println("Message:", string(m.Data))
		err, _ := handleMessage(m)
		if err != nil {
			log.Println(err)
		}
	}
	sub, err := sc.Subscribe(topic, subscribeCallback, stan.DurableName("my-durable")) // durable name функция создает подписку с памятью у стэна
	// гарантирует доставку сообщений подписка с гарантированной доставкой
	if err != nil {
		log.Fatal(err)
	}

	defer sub.Unsubscribe()

	for {
		sc.Publish(topic, []byte(sd.JsonData))
		var t = time.NewTimer(time.Second * 5).C
		ts := <-t
		fmt.Println("Timer:", ts)
		// вызов таймера каналы в Го уникальная вещь это объект в который можно объект послать и считать
	}
}

var ErrMyError = errors.New("my error")

func handleMessage(m *stan.Msg) (error, bool) {
	log.Printf("Received message length: %d \n", len(m.Data))
	return nil, false
	// все функции должны возвращать
	//return ErrMyError, false
}
