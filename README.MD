# Kafka

Kafka是在sarama基础上封装的快速使用包。可快捷的使用Producer发送消息至kafka，可以便捷的使用Consumer消费指定topics的kafka消息。

## 安装

```shell
go get -u "github.com/sk-pkg/kafka"
```

## 快速开始
```go
package main

import (
	"github.com/Shopify/sarama"
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
		k.SendMessage("kafka-test", sarama.StringEncoder("testKey:"+num), sarama.StringEncoder("testValue:"+num))
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
```
## 可配置项
1.ClientID连接客户端ID，此项为选填，如果此项不为空时将会生成"yourClient_hostname"形式的连接客户端ID
```go
kafka.WithClientID("kafka")
```
2.Version指定使用的kafka版本号，默认为（v3.5.1.0），此项可选
```go
kafka.WithVersion(sarama.V3_5_1_0)
```
3.ProducerBrokers指定Producer连接的brokers，如要生产kafka消息是此项为必填
```go
kafka.WithProducerBrokers([]string{"c02-01.kafka.k8s.local:9092", "c02-02.kafka.k8s.local:9092"})
```
4.ProducerRetryMax消息发送失败重试次数，默认值为1，此项可选
```go
kafka.WithProducerRetryMax(10)
```
5.ConsumerBrokers指定Consumer连接的brokers，如要消费kafka消息是此项为必填
```go
kafka.WithConsumerBrokers([]string{"c02-01.kafka.k8s.local:9092", "c02-02.kafka.k8s.local:9092"})
```
6.ConsumerTopics需要消费的topic，如要消费kafka消息是此项为必填
```go
kafka.WithConsumerTopics([]string{"kafka-test1", "kafka-test2"})
```
7.ConsumerGroup消费group，如要消费kafka消息是此项为必填
```go
kafka.WithConsumerGroup("test-group")
```
8.ConsumerOffsetsRetryMax在OffsetManager关闭期间重试失败提交、请求的总次数，默认为1，此项可选
```go
kafka.WithConsumerOffsetsRetryMax(10)
```
9.WithLogger日志实例，此项为必填
```go
kafka.WithLogger(logger *zap.Logger)
```
10.WithAutoSubmit消费自动提交，默认为true。当值为true时，接收到消息后会自动提交，此时Manager.ConsumerMessages有消息。当值为false时，Manager.Consumers有消费者。
```go
kafka.WithAutoSubmit(false)
```
11.Close()关闭kafka，为了避免生产者丢消息，务必在关闭程序时调用此方法
```go
kafka.Close()
```

## 可用方法
1.SendMessage 发送一条<key,value>到kafka指定topic中
```go
kafkaClient.SendMessage("kafka-test", sarama.StringEncoder("testKey"+strI), sarama.StringEncoder("testValue"+strI))
```
2.自动消费一条kafka消息
```go
for {
	time.Sleep(time.Second)
	msg := <-kafkaClient.ConsumerMessages
	getLogger().Info("A message was received", zap.Any("value", sarama.StringEncoder(msg.Value)))
}
```
3.手动消费一条kafka消息
```go
for {
		select {
		// 取一个应答
		case c := <-kafkaClient.Consumers:
			// 获取消息
			msg := c.GetMsg()
			getLogger().Info("msg", zap.Any("msg", msg))
			// 手动提交
			c.Submit()
		}
	}
```
## 使用sarama原生方法
```go
producer := kafkaClient.Producer
producer.Input() <- &sarama.ProducerMessage{
    Topic: topic,
    Key:   key,
    Value: value,
}

consumerGroup := kafkaClient.ConsumerGroup
ctx := context.Background()
for {
    if err := consumerGroup.Consume(ctx, topics, kafkaClient); err != nil {
        m.Logger.Error("Consume Error", zap.Error(err))
        return
    }
}

for {
    time.Sleep(time.Second)
    consumerMessages := <-kafkaClient.ConsumerMessages
    getLogger().Info("A message was received", zap.Any("value", sarama.StringEncoder(consumerMessages.Value)))
}
```