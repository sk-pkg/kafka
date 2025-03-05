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
		kafka.WithAutoSubmit(false), // Manual commit mode
		kafka.WithLogger(loggers))
	if err != nil {
		loggers.Fatal("kafka producer init error:" + err.Error())
	}

	// Start message producer in a separate goroutine
	go producer(kafkaClient)
	// Uncomment to use auto-commit consumer
	// autoConsumer(kafkaClient)
	// Start manual-commit consumer
	consumer(kafkaClient)

	time.Sleep(200 * time.Second)
}

// producer demonstrates how to send messages to a Kafka topic.
// It sends 5 messages with both key and value, waiting 1 second between each message.
//
// Parameters:
//   - k: A pointer to the kafka Manager instance.
//
// Example message format:
//   - Key: "testKey:0", "testKey:5", ...
//   - Value: "testValue:0", "testValue:5", ...
func producer(k *kafka.Manager) {
	for i := 0; i < 5; i++ {
		time.Sleep(1 * time.Second)
		k.Logger.Info("send message", zap.Any("number", i))
		num := strconv.Itoa(i * 5)
		k.SendMessage("kafka-test", sarama.StringEncoder("testKey:"+num), sarama.StringEncoder("testValue:"+num))
	}
}

// consumer demonstrates manual message consumption from Kafka.
// It continuously reads messages from the Consumers channel and manually commits them
// after processing.
//
// Parameters:
//   - k: A pointer to the kafka Manager instance.
//
// Note: This function requires WithAutoSubmit(false) to be set during Manager initialization.
func consumer(k *kafka.Manager) {
	for {
		select {
		// Get one message
		case c := <-k.Consumers:
			getLogger().Info("msg", zap.Any("msg", c.GetMsg()))
			// Manually commit the message
			c.Submit()
		}
	}
}

// autoConsumer demonstrates automatic message consumption from Kafka.
// It continuously reads messages from the ConsumerMessages channel without
// needing to manually commit offsets.
//
// Parameters:
//   - k: A pointer to the kafka Manager instance.
//
// Note: This function requires WithAutoSubmit(true) to be set during Manager initialization.
func autoConsumer(k *kafka.Manager) {
	for {
		select {
		// Get one message
		case msg := <-k.ConsumerMessages:
			getLogger().Info("msg", zap.Any("msg", msg))
		}
	}
}

// getLogger initializes and returns a configured zap logger instance.
//
// Returns:
//   - *zap.Logger: A configured logger instance.
//
// The function will terminate the program if logger initialization fails.
func getLogger() *zap.Logger {
	loggers, err := logger.New()
	if err != nil {
		log.Fatal(err)
	}

	return loggers
}
