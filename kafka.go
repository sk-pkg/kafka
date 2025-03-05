// Copyright 2024 Seakee. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

// Package kafka provides a high-level wrapper for the Sarama Kafka client library.
// It simplifies the process of producing messages to and consuming messages from Kafka topics.
// This package supports both synchronous and asynchronous message production and consumption.
package kafka

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"os"
)

const (
	defaultRetryMax   = 1     // Default maximum number of retries for producer and consumer
	defaultAutoSubmit = true  // Default setting for auto-submitting consumed messages
)

type (
	// Option is a function type that modifies the options for Kafka client configuration.
	Option func(*option)

	// option contains all configuration parameters for the Kafka client.
	option struct {
		clientID                string              // Client identifier for Kafka requests
		version                 sarama.KafkaVersion // Kafka version compatibility
		producerBrokers         []string            // List of broker addresses for the producer
		producerRetryMax        int                 // Maximum number of retries for producer operations
		consumerBrokers         []string            // List of broker addresses for the consumer
		consumerTopics          []string            // List of topics to consume from
		consumerGroup           string              // Consumer group identifier
		consumerOffsetsRetryMax int                 // Maximum number of retries for consumer offset commits
		logger                  *zap.Logger         // Logger for recording operations and errors
		autoSubmit              bool                // Whether to automatically mark messages as processed
	}

	// Manager is the main struct that handles Kafka operations.
	// It contains both producer and consumer instances and manages their lifecycle.
	Manager struct {
		Producer         sarama.AsyncProducer         // Asynchronous producer instance
		ConsumerGroup    sarama.ConsumerGroup         // Consumer group instance
		ConsumerMessages chan *sarama.ConsumerMessage // Channel for consumed messages when autoSubmit is true
		Consumers        chan Consumer                // Channel for consumer instances when autoSubmit is false
		Logger           *zap.Logger                  // Logger for recording operations and errors
		option           *option                      // Configuration options
	}

	// consumer implements the Consumer interface and wraps a Kafka message and session.
	consumer struct {
		message *sarama.ConsumerMessage       // The consumed message
		session sarama.ConsumerGroupSession   // The session for marking messages as processed
	}

	// Consumer is an interface that provides methods to access consumed messages
	// and mark them as processed.
	Consumer interface {
		// GetMsg returns the consumed Kafka message.
		GetMsg() *sarama.ConsumerMessage
		
		// Submit marks the message as processed.
		Submit()
	}
)

// GetMsg returns the consumed Kafka message.
// This method is part of the Consumer interface implementation.
//
// Returns:
//   - *sarama.ConsumerMessage: The consumed Kafka message.
func (c *consumer) GetMsg() *sarama.ConsumerMessage {
	return c.message
}

// Submit marks the message as processed in the Kafka consumer group.
// This method is part of the Consumer interface implementation.
// It should be called after successfully processing a message to update the consumer offset.
func (c *consumer) Submit() {
	c.session.MarkMessage(c.message, "done")
}

// WithAutoSubmit sets whether messages should be automatically marked as processed.
//
// Parameters:
//   - auto: If true, messages will be automatically marked as processed after being sent to the ConsumerMessages channel.
//           If false, the application must explicitly call Submit() on each Consumer instance.
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithAutoSubmit(false), // Require manual message acknowledgment
//	    // other options...
//	)
func WithAutoSubmit(auto bool) Option {
	return func(o *option) {
		o.autoSubmit = auto
	}
}

// WithClientID sets the client identifier for Kafka requests.
// The hostname of the machine will be appended to the provided ID.
//
// Parameters:
//   - CID: The base client ID to use.
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithClientID("my-application"),
//	    // other options...
//	)
func WithClientID(CID string) Option {
	return func(o *option) {
		hostname, _ := os.Hostname()
		o.clientID = CID + "-" + hostname
	}
}

// WithVersion sets the Kafka protocol version to use.
//
// Parameters:
//   - version: The Kafka version to use for compatibility.
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithVersion(sarama.V2_8_0_0),
//	    // other options...
//	)
func WithVersion(version sarama.KafkaVersion) Option {
	return func(o *option) {
		o.version = version
	}
}

// WithProducerBrokers sets the list of Kafka brokers for the producer.
//
// Parameters:
//   - brokers: A slice of strings containing the addresses of Kafka brokers (e.g., ["localhost:9092"]).
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithProducerBrokers([]string{"kafka1:9092", "kafka2:9092"}),
//	    // other options...
//	)
func WithProducerBrokers(brokers []string) Option {
	return func(o *option) {
		o.producerBrokers = brokers
	}
}

// WithProducerRetryMax sets the maximum number of retries for producer operations.
//
// Parameters:
//   - retryMax: The maximum number of times to retry sending a message before giving up.
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithProducerRetryMax(3), // Retry failed messages up to 3 times
//	    // other options...
//	)
func WithProducerRetryMax(retryMax int) Option {
	return func(o *option) {
		o.producerRetryMax = retryMax
	}
}

// WithConsumerBrokers sets the list of Kafka brokers for the consumer.
//
// Parameters:
//   - brokers: A slice of strings containing the addresses of Kafka brokers (e.g., ["localhost:9092"]).
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithConsumerBrokers([]string{"kafka1:9092", "kafka2:9092"}),
//	    // other options...
//	)
func WithConsumerBrokers(brokers []string) Option {
	return func(o *option) {
		o.consumerBrokers = brokers
	}
}

// WithConsumerTopics sets the list of topics to consume messages from.
//
// Parameters:
//   - topics: A slice of strings containing the names of Kafka topics to consume from.
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithConsumerTopics([]string{"orders", "notifications"}),
//	    // other options...
//	)
func WithConsumerTopics(topics []string) Option {
	return func(o *option) {
		o.consumerTopics = topics
	}
}

// WithConsumerGroup sets the consumer group identifier.
// Consumer groups allow multiple consumers to coordinate and balance the consumption of topics.
//
// Parameters:
//   - group: The name of the consumer group.
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithConsumerGroup("order-processing-group"),
//	    // other options...
//	)
func WithConsumerGroup(group string) Option {
	return func(o *option) {
		o.consumerGroup = group
	}
}

// WithConsumerOffsetsRetryMax sets the maximum number of retries for consumer offset commits.
//
// Parameters:
//   - retryMax: The maximum number of times to retry committing offsets before giving up.
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	manager, err := kafka.New(
//	    kafka.WithConsumerOffsetsRetryMax(5),
//	    // other options...
//	)
func WithConsumerOffsetsRetryMax(retryMax int) Option {
	return func(o *option) {
		o.consumerOffsetsRetryMax = retryMax
	}
}

// WithLogger sets the logger for recording operations and errors.
//
// Parameters:
//   - logger: A zap.Logger instance for logging.
//
// Returns:
//   - Option: A function that modifies the Kafka client configuration.
//
// Example:
//
//	logger, _ := zap.NewProduction()
//	manager, err := kafka.New(
//	    kafka.WithLogger(logger),
//	    // other options...
//	)
func WithLogger(logger *zap.Logger) Option {
	return func(o *option) {
		o.logger = logger
	}
}

// New creates a new Kafka manager with the provided options.
// It initializes both producer and consumer based on the configuration.
//
// Parameters:
//   - opts: A variadic list of Option functions to configure the Kafka client.
//
// Returns:
//   - *Manager: A pointer to the initialized Manager instance.
//   - error: An error if initialization fails, nil otherwise.
//
// Example:
//
//	// Initialize a producer-only manager
//	producerManager, err := kafka.New(
//	    kafka.WithProducerBrokers([]string{"kafka:9092"}),
//	    kafka.WithLogger(logger),
//	)
//
//	// Initialize a consumer-only manager
//	consumerManager, err := kafka.New(
//	    kafka.WithConsumerBrokers([]string{"kafka:9092"}),
//	    kafka.WithConsumerGroup("my-group"),
//	    kafka.WithConsumerTopics([]string{"my-topic"}),
//	    kafka.WithLogger(logger),
//	)
func New(opts ...Option) (*Manager, error) {
	// Initialize options with default values
	opt := &option{
		version:                 sarama.V3_5_1_0,
		producerRetryMax:        defaultRetryMax,
		consumerOffsetsRetryMax: defaultRetryMax,
		autoSubmit:              defaultAutoSubmit,
	}

	// Apply all provided options
	for _, f := range opts {
		f(opt)
	}

	// Create the manager with the configured logger
	manager := &Manager{Logger: opt.logger, option: opt}

	// Validate that at least one of producer or consumer brokers is provided
	if len(opt.producerBrokers) == 0 && len(opt.consumerBrokers) == 0 {
		return manager, errors.New("producer Brokers和consumer Brokers不能同时为空")
	}

	// Configure Sarama client
	conf := sarama.NewConfig()
	conf.Version = opt.version
	conf.Producer.Return.Successes = true                 // Receive success responses from the server
	conf.Producer.Return.Errors = true                    // Receive error responses from the server
	conf.Producer.Retry.Max = opt.producerRetryMax        // Number of times to retry sending a message
	conf.ClientID = opt.clientID                          // Client ID to pass to the server with requests
	conf.Producer.RequiredAcks = sarama.WaitForLocal      // Wait for the local commit only
	conf.Producer.Partitioner = sarama.NewHashPartitioner // Use hash of key to determine partition
	conf.Consumer.Offsets.Retry.Max = opt.consumerOffsetsRetryMax
	conf.Consumer.Return.Errors = true

	// Initialize the asynchronous producer if broker addresses are provided
	if len(opt.producerBrokers) > 0 {
		producer, err := sarama.NewAsyncProducer(opt.producerBrokers, conf)
		if err != nil {
			return manager, err
		}

		// Start a goroutine to handle producer responses
		go func(p sarama.AsyncProducer) {
			for {
				select {
				case err = <-p.Errors():
					// Log any errors that occur during message production
					if err != nil {
						opt.logger.Error("send msg to kafka failed:", zap.Error(err))
					}
				case <-p.Successes():
					// Drain the successes channel to prevent it from filling up
				}
			}
		}(producer)

		manager.Producer = producer
	}

	// Initialize the consumer group if broker addresses are provided
	if len(opt.consumerBrokers) > 0 {
		group, err := sarama.NewConsumerGroup(opt.consumerBrokers, opt.consumerGroup, conf)
		if err != nil {
			return manager, err
		}

		opt.logger.Info("Consumer Group init success")

		// Create appropriate channel based on auto-submit setting
		if opt.autoSubmit {
			manager.ConsumerMessages = make(chan *sarama.ConsumerMessage, 256)
		} else {
			manager.Consumers = make(chan Consumer, 256)
		}

		// Start the consumer loop in a separate goroutine
		go manager.consumerLoop(opt.consumerTopics)

		manager.ConsumerGroup = group
	}

	return manager, nil
}

// SendMessage sends a message with the specified key and value to the given Kafka topic.
// This method uses the asynchronous producer to send messages.
//
// Parameters:
//   - topic: The Kafka topic to send the message to.
//   - key: The message key, used for partitioning.
//   - value: The message value (payload).
//
// Example:
//
//	// Send a string message with a string key
//	manager.SendMessage("my-topic", 
//	    sarama.StringEncoder("user-123"), 
//	    sarama.StringEncoder(`{"name":"John","action":"login"}`))
func (m *Manager) SendMessage(topic string, key, value sarama.Encoder) {
	// Log the message being sent
	m.Logger.Debug("Received a message", zap.Any("key", key), zap.Any("value", value))

	// Send the message to the producer's input channel
	m.Producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: value,
	}
}

// consumerLoop continuously consumes messages from the specified Kafka topics.
// This method is typically started as a goroutine during initialization.
//
// Parameters:
//   - topics: A slice of topic names to consume from.
func (m *Manager) consumerLoop(topics []string) {
	ctx := context.Background()
	m.Logger.Info("Consumer started")
	
	// Continuously attempt to consume messages
	for {
		// The Consume method blocks until the consumer is rebalanced or encounters an error
		if err := m.ConsumerGroup.Consume(ctx, topics, m); err != nil {
			m.Logger.Error("Consume Error", zap.Error(err))
			return
		}
	}
}

// ConsumeClaim implements the ConsumerGroupHandler interface for message consumption.
// It processes messages from a single partition and handles them based on the autoSubmit configuration.
//
// Parameters:
//   - s: The consumer group session for managing message offsets.
//   - c: The claim containing messages from a single partition.
//
// Returns:
//   - error: An error if message processing fails, nil otherwise.
//
// Example:
//
//	// This method is called automatically by the consumer group
//	// When autoSubmit is true:
//	//   Messages are automatically marked as processed and sent to ConsumerMessages channel
//	// When autoSubmit is false:
//	//   Messages are wrapped in a Consumer interface and sent to Consumers channel
func (m *Manager) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		// Process only non-nil messages
		if msg != nil {
			// Handle auto-submit mode
			if m.option.autoSubmit {
				s.MarkMessage(msg, "done")
				m.ConsumerMessages <- msg
				return nil
			}

			// Handle manual-submit mode
			m.Consumers <- &consumer{
				message: msg,
				session: s,
			}
		}
	}

	return nil
}

// Setup implements the ConsumerGroupHandler interface.
// It is called before consuming begins and can be used for setup tasks.
//
// Parameters:
//   - sarama.ConsumerGroupSession: The session for this consumer group.
//
// Returns:
//   - error: An error if setup fails, nil otherwise.
func (m *Manager) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup implements the ConsumerGroupHandler interface.
// It is called after all ConsumeClaim goroutines have exited and can be used for cleanup tasks.
//
// Parameters:
//   - sarama.ConsumerGroupSession: The session for this consumer group.
//
// Returns:
//   - error: An error if cleanup fails, nil otherwise.
func (m *Manager) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// Close gracefully shuts down both the producer and consumer group instances.
// It should be called when the Manager is no longer needed to prevent resource leaks.
//
// Example:
//
//	manager, _ := kafka.New(...)
//	defer manager.Close()
func (m *Manager) Close() {
	var err error
	
	// Close the producer if it exists
	if m.Producer != nil {
		err = m.Producer.Close()
		if err != nil {
			m.Logger.Error("Close Producer failed", zap.Error(err))
		}
	}

	// Close the consumer group if it exists
	if m.ConsumerGroup != nil {
		err = m.ConsumerGroup.Close()
		if err != nil {
			m.Logger.Error("Close ConsumerGroup failed", zap.Error(err))
		}
	}
}
