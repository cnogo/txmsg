package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/jmoiron/sqlx"
	"log"
	"time"
	"txmsg"
)

func main() {


	cfg := &txmsg.Config{
		EtcdHosts: []string{"http://localhost:2379"},
		RocketMQAddrs: []string{"192.168.50.121:9876"},
		DeliveryWorkNum: 10,
	}


	dbUrl := fmt.Sprintf("%s:%s@(%s:%d)/%s?parseTime=true&loc=Local",
		"root",
		"123456",
		"127.0.0.1",
		10001,
		"test")
	db, err := sqlx.Connect("mysql", dbUrl)


	if err != nil {
		log.Println(err.Error())
		return
	}

	err = db.Ping()
	if err != nil {
		log.Println(err)
		return
	}


	txmsgCli, err := txmsg.NewTxMsgClient(db, []string{"hellotopic"}, cfg)
	if err != nil {
		log.Println(err)
		return
	}

	err = txmsgCli.Init()

	if err != nil {
		log.Println(err)
		return
	}


	tx, err := db.Begin()
	if err != nil {
		log.Println(err)
		return
	}

	_, err = tx.Exec("insert into demo(test) values(?)", "test x")
	if err != nil {
		tx.Rollback()
		log.Println(err)
		return
	}

	cnt, err := txmsgCli.SendMsg( "hello cotent" + time.Now().String(), "hellotopic", "hellotag", 0)
	if err != nil {
		log.Println(err)
		tx.Rollback()
		return
	}
	fmt.Println(cnt)

	tx.Commit()

	go ConsumerTest()

	select {
	}
}

func ConsumerTest() {
	c, err :=rocketmq.NewPushConsumer(consumer.WithNameServer([]string{"192.168.50.121:9876"}),
		consumer.WithGroupName("TransactionMsgConsumer"))
	if err != nil {
		log.Println(err)
		return
	}

	err = c.Subscribe("hellotopic", consumer.MessageSelector{},
		func(context context.Context, msgs ...*primitive.MessageExt) (result consumer.ConsumeResult, e error) {
			log.Println("begin range topic hello")
			log.Println(len(msgs))

			for i := 0; i < len(msgs); i++ {
				fmt.Printf("%s\n", msgs[i].Body)
			}

			log.Println("end range topic hello")
			return consumer.ConsumeSuccess, nil
		})

	if err != nil {
		log.Println(err)
		return
	}

	err = c.Start()
	defer c.Shutdown()

	if err != nil {
		log.Println(err)
		return
	}

	select {

	}


}