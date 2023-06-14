package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sk-pkg/logger"
	"go.uber.org/zap"
	"log"
	"strconv"
	"testing"
	"time"
)

func getLogger() *zap.Logger {
	loggers, err := logger.New()
	if err != nil {
		log.Fatal(err)
	}

	return loggers
}

func TestKafkaProducer(t *testing.T) {

	kafkaClient, err := New(WithClientID("kafka-test"),
		WithProducerBrokers([]string{"c02-01.kafka.k8s.local:9092", "c02-02.kafka.k8s.local:9092", "c02-03.kafka.k8s.local:9092"}),
		WithLogger(getLogger()))
	if err != nil {
		t.Fatal("kafka producer init error:" + err.Error())
	}

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		kafkaClient.Logger.Info("send message", zap.Any("number", i))
		strI := strconv.Itoa(i)
		kafkaClient.SendMessage("kafka-test", sarama.StringEncoder("testKey"+strI), sarama.StringEncoder("testValue"+strI))
	}
}

func TestKafkaConsumer(t *testing.T) {
	kafkaClient, err := New(WithClientID("kafka-test"),
		WithConsumerTopics([]string{"kafka-test"}),
		WithConsumerGroup("kafka-test"),
		WithConsumerBrokers([]string{"c02-01.kafka.k8s.local:9092", "c02-02.kafka.k8s.local:9092", "c02-03.kafka.k8s.local:9092"}),
		WithLogger(getLogger()))
	if err != nil {
		t.Fatal("kafka consumer init error:" + err.Error())
	}

	for {
		time.Sleep(time.Second)
		msg := <-kafkaClient.ConsumerMessages
		getLogger().Info("A message was received", zap.Any("value", sarama.StringEncoder(msg.Value)))
	}
}
