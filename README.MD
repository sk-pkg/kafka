# Kafka Client Documentation

This is a Kafka client library built on top of [sarama](https://github.com/IBM/sarama), providing simple APIs for producing and consuming Kafka messages.

## Installation

Install using Go modules:

```bash
go get -u "github.com/sk-pkg/kafka"
```

## Quick Start

Here's a simple example showing how to initialize the Kafka client and use it to produce and consume messages:

```go
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
        kafka.WithAutoSubmit(false), // Required for manual commit mode
        kafka.WithLogger(loggers))
    if err != nil {
        loggers.Fatal("kafka client init error:" + err.Error())
    }

    // Produce messages
    go producer(kafkaClient)
    
    // Consume messages (choose one mode)
    // Auto-commit mode
    // autoConsumer(kafkaClient)
    // Manual commit mode
    consumer(kafkaClient)
    
    // Close connection before program exits
    defer kafkaClient.Close()
    
    time.Sleep(200 * time.Second)
}
```

## Configuration Options

### Client Configuration

```go
// Set client ID (optional)
kafka.WithClientID("kafka")

// Set Kafka version (optional, default: v3.5.1.0)
kafka.WithVersion(sarama.V3_5_1_0)

// Set producer brokers (required for producing messages)
kafka.WithProducerBrokers([]string{"c01-01.kafka.local:9092", "c01-02.kafka.local:9092"})

// Set producer retry max (optional, default: 1)
kafka.WithProducerRetryMax(10)

// Set consumer brokers (required for consuming messages)
kafka.WithConsumerBrokers([]string{"c01-01.kafka.local:9092", "c01-02.kafka.local:9092"})

// Set consumer topics (required for consuming messages)
kafka.WithConsumerTopics([]string{"kafka-test1", "kafka-test2"})

// Set consumer group (required for consuming messages)
kafka.WithConsumerGroup("test-group")

// Set consumer offsets retry max (optional, default: 1)
kafka.WithConsumerOffsetsRetryMax(10)

// Set logger instance (required)
kafka.WithLogger(logger *zap.Logger)

// Set auto-commit mode (optional, default: true)
kafka.WithAutoSubmit(false)
```

## Producing Messages

```go
func producer(k *kafka.Manager) {
    for i := 0; i < 5; i++ {
        time.Sleep(1 * time.Second)
        k.Logger.Info("send message", zap.Any("number", i))
        strI := strconv.Itoa(i)
        k.SendMessage("kafka-test", sarama.StringEncoder("testKey"+strI), sarama.StringEncoder("testValue"+strI))
    }
}
```

## Consuming Messages

### Auto-commit Mode

```go
func autoConsumer(k *kafka.Manager) {
    for {
        select {
        case msg := <-k.ConsumerMessages:
            getLogger().Info("msg", zap.Any("msg", msg))
        }
    }
}
```

### Manual Commit Mode

```go
func consumer(k *kafka.Manager) {
    for {
        select {
        case c := <-k.Consumers:
            msg := c.GetMsg()
            getLogger().Info("msg", zap.Any("msg", msg))
            c.Submit()
        }
    }
}
```

## Closing Connection

```go
defer kafkaClient.Close()
```

## Using Native Sarama Methods

```go
// Using native Producer
producer := kafkaClient.Producer
producer.Input() <- &sarama.ProducerMessage{
    Topic: topic,
    Key:   key,
    Value: value,
}

// Using native ConsumerGroup
consumerGroup := kafkaClient.ConsumerGroup
ctx := context.Background()
for {
    if err := consumerGroup.Consume(ctx, topics, kafkaClient); err != nil {
        logger.Error("Consume Error", zap.Error(err))
        return
    }
}

// Getting messages
for {
    time.Sleep(time.Second)
    consumerMessages := <-kafkaClient.ConsumerMessages
    logger.Info("A message was received", zap.Any("value", sarama.StringEncoder(consumerMessages.Value)))
}
```

## Complete Examples

### Producer Example

```go
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
        kafka.WithLogger(loggers))
    if err != nil {
        loggers.Fatal("kafka producer init error:" + err.Error())
    }
    
    for i := 0; i < 10; i++ {
        time.Sleep(time.Second)
        kafkaClient.Logger.Info("send message", zap.Any("number", i))
        strI := strconv.Itoa(i)
        kafkaClient.SendMessage("kafka-test", sarama.StringEncoder("testKey"+strI), sarama.StringEncoder("testValue"+strI))
    }
    
    defer kafkaClient.Close()
}
```

### Auto-commit Consumer Example

```go
package main

import (
    "github.com/IBM/sarama"
    "github.com/sk-pkg/kafka"
    "github.com/sk-pkg/logger"
    "go.uber.org/zap"
    "log"
    "time"
)

func main() {
    loggers := getLogger()
    kafkaClient, err := kafka.New(kafka.WithClientID("kafka-test"),
        kafka.WithConsumerTopics([]string{"kafka-test"}),
        kafka.WithConsumerGroup("kafka-test"),
        kafka.WithConsumerBrokers([]string{"c01-01.kafka.local:9092", "c01-02.kafka.local:9092", "c01-03.kafka.local:9092"}),
        kafka.WithLogger(loggers))
    if err != nil {
        loggers.Fatal("kafka consumer init error:" + err.Error())
    }
    
    for {
        time.Sleep(time.Second)
        msg := <-kafkaClient.ConsumerMessages
        loggers.Info("A message was received", zap.Any("value", sarama.StringEncoder(msg.Value)))
    }
    
    defer kafkaClient.Close()
}
```

### Manual Commit Consumer Example

```go
package main

import (
    "github.com/IBM/sarama"
    "github.com/sk-pkg/kafka"
    "github.com/sk-pkg/logger"
    "go.uber.org/zap"
    "log"
    "time"
)

func main() {
    loggers := getLogger()
    kafkaClient, err := kafka.New(kafka.WithClientID("kafka-test"),
        kafka.WithConsumerTopics([]string{"kafka-test"}),
        kafka.WithConsumerGroup("kafka-test"),
        kafka.WithConsumerBrokers([]string{"c01-01.kafka.local:9092", "c01-02.kafka.local:9092", "c01-03.kafka.local:9092"}),
        kafka.WithAutoSubmit(false),
        kafka.WithLogger(loggers))
    if err != nil {
        loggers.Fatal("kafka consumer init error:" + err.Error())
    }
    
    for {
        select {
        case c := <-kafkaClient.Consumers:
            msg := c.GetMsg()
            loggers.Info("msg", zap.Any("msg", msg))
            c.Submit()
        }
    }
    
    defer kafkaClient.Close()
}
```