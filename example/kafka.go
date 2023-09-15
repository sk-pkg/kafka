package main

import (
	"github.com/IBM/sarama"
	"github.com/sk-pkg/kafka"
	"github.com/sk-pkg/logger"
	"go.uber.org/zap"
	"log"
	"strconv"
	"time"
)

func main() {
	loggers := getLogger()
	kafkaClient, err := kafka.New(kafka.WithClientID("kafka-test"),
		kafka.WithProducerBrokers([]string{"c01-01.kafka.local:9092", "c01-02.kafka.local:9092", "c01-03.kafka.local:9092"}),
		kafka.WithConsumerTopics([]string{"kafka-test"}),
		kafka.WithConsumerGroup("kafka-test"),
		kafka.WithConsumerBrokers([]string{"c01-01.kafka.local:9092", "c01-02.kafka.local:9092", "c01-03.kafka.local:9092"}),
		kafka.WithAutoSubmit(false), // 手动提交时需要设置为false
		kafka.WithLogger(loggers))
	if err != nil {
		loggers.Fatal("kafka producer init error:" + err.Error())
	}

	// 生产消息
	go producer(kafkaClient)
	// 自动提交消费
	// autoConsumer(kafkaClient)
	// 手动提交消费
	consumer(kafkaClient)

	time.Sleep(200 * time.Second)
}

func producer(k *kafka.Manager) {
	for i := 0; i < 5; i++ {
		time.Sleep(1 * time.Second)
		k.Logger.Info("send message", zap.Any("number", i))
		num := strconv.Itoa(i * 5)
		k.SendMessage("datacenter-source-data", sarama.StringEncoder("testKey:"+num), sarama.StringEncoder("testValue:"+num))
	}
}

// 手动提交
func consumer(k *kafka.Manager) {
	for {
		select {
		// 取一个应答
		case c := <-k.Consumers:
			getLogger().Info("msg", zap.Any("msg", c.GetMsg()))
			// 手动提交
			c.Submit()
		}
	}
}

// 自动提交
func autoConsumer(k *kafka.Manager) {
	for {
		select {
		// 取一个应答
		case msg := <-k.ConsumerMessages:
			getLogger().Info("msg", zap.Any("msg", msg))
		}
	}
}

func getLogger() *zap.Logger {
	loggers, err := logger.New()
	if err != nil {
		log.Fatal(err)
	}

	return loggers
}
