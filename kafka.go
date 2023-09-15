package kafka

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"os"
)

const (
	defaultRetryMax   = 1
	defaultAutoSubmit = true
)

type (
	Option func(*option)

	option struct {
		clientID                string
		version                 sarama.KafkaVersion // 版本
		producerBrokers         []string            // 生产者连接地址
		producerRetryMax        int                 // 生产者最大重试次数
		consumerBrokers         []string            // 消费者连接地址
		consumerTopics          []string            // 消费主题
		consumerGroup           string              // 消费者组
		consumerOffsetsRetryMax int                 // 消费最大重试次数
		logger                  *zap.Logger         // 日志
		autoSubmit              bool                // 消费时自动提交
	}

	Manager struct {
		Producer         sarama.AsyncProducer         // 生产者
		ConsumerGroup    sarama.ConsumerGroup         // 消费组
		ConsumerMessages chan *sarama.ConsumerMessage // 消息，当autoSubmit为true时生效
		Consumers        chan Consumer                // 消费者，当autoSubmit为false时生效
		Logger           *zap.Logger
		option           *option
	}

	consumer struct {
		message *sarama.ConsumerMessage
		session sarama.ConsumerGroupSession
	}

	Consumer interface {
		GetMsg() *sarama.ConsumerMessage
		Submit()
	}
)

func (c *consumer) GetMsg() *sarama.ConsumerMessage {
	return c.message
}

func (c *consumer) Submit() {
	c.session.MarkMessage(c.message, "done")
}

func WithAutoSubmit(auto bool) Option {
	return func(o *option) {
		o.autoSubmit = auto
	}
}

func WithClientID(CID string) Option {
	return func(o *option) {
		hostname, _ := os.Hostname()
		o.clientID = CID + "-" + hostname
	}
}

func WithVersion(version sarama.KafkaVersion) Option {
	return func(o *option) {
		o.version = version
	}
}

func WithProducerBrokers(brokers []string) Option {
	return func(o *option) {
		o.producerBrokers = brokers
	}
}

func WithProducerRetryMax(retryMax int) Option {
	return func(o *option) {
		o.producerRetryMax = retryMax
	}
}

func WithConsumerBrokers(brokers []string) Option {
	return func(o *option) {
		o.consumerBrokers = brokers
	}
}

func WithConsumerTopics(topics []string) Option {
	return func(o *option) {
		o.consumerTopics = topics
	}
}

func WithConsumerGroup(group string) Option {
	return func(o *option) {
		o.consumerGroup = group
	}
}

func WithConsumerOffsetsRetryMax(retryMax int) Option {
	return func(o *option) {
		o.consumerOffsetsRetryMax = retryMax
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(o *option) {
		o.logger = logger
	}
}

func New(opts ...Option) (*Manager, error) {
	opt := &option{
		version:                 sarama.V3_5_1_0,
		producerRetryMax:        defaultRetryMax,
		consumerOffsetsRetryMax: defaultRetryMax,
		autoSubmit:              defaultAutoSubmit,
	}

	for _, f := range opts {
		f(opt)
	}

	manager := &Manager{Logger: opt.logger, option: opt}

	if len(opt.producerBrokers) == 0 && len(opt.consumerBrokers) == 0 {
		return manager, errors.New("producer Brokers和consumer Brokers不能同时为空")
	}

	conf := sarama.NewConfig()
	conf.Version = opt.version
	conf.Producer.Return.Successes = true                 // 接收服务器反馈的成功响应
	conf.Producer.Return.Errors = true                    // 接收服务器反馈的失败响应
	conf.Producer.Retry.Max = opt.producerRetryMax        // 消息发送失败重试次数
	conf.ClientID = opt.clientID                          // 发送请求时传递给服务端的ClientID. 用来追溯请求源
	conf.Producer.RequiredAcks = sarama.WaitForLocal      // 只等待leader保存成功后的响应
	conf.Producer.Partitioner = sarama.NewHashPartitioner // 通过msg中的key生成hash值,选择分区
	conf.Consumer.Offsets.Retry.Max = opt.consumerOffsetsRetryMax
	conf.Consumer.Return.Errors = true

	// 初始化一个异步的Producer
	if len(opt.producerBrokers) > 0 {
		producer, err := sarama.NewAsyncProducer(opt.producerBrokers, conf)
		if err != nil {
			return manager, err
		}

		go func(p sarama.AsyncProducer) {
			for {
				select {
				case err = <-p.Errors():
					if err != nil {
						opt.logger.Error("send msg to kafka failed:", zap.Error(err))
					}
				case <-p.Successes():
				}
			}
		}(producer)

		manager.Producer = producer
	}

	// 初始化指定消费组
	if len(opt.consumerBrokers) > 0 {
		group, err := sarama.NewConsumerGroup(opt.consumerBrokers, opt.consumerGroup, conf)
		if err != nil {
			return manager, err
		}

		opt.logger.Info("Consumer Group init success")

		if opt.autoSubmit {
			manager.ConsumerMessages = make(chan *sarama.ConsumerMessage, 256)
		} else {
			manager.Consumers = make(chan Consumer, 256)
		}

		go manager.consumerLoop(opt.consumerTopics)

		manager.ConsumerGroup = group
	}

	return manager, nil
}

// SendMessage 发送一条<key,value>到kafka指定topic中
func (m *Manager) SendMessage(topic string, key, value sarama.Encoder) {
	m.Logger.Debug("Received a message", zap.Any("key", key), zap.Any("value", value))

	m.Producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: value,
	}
}

// consumerLoop 循环从kafka获取数据
func (m *Manager) consumerLoop(topics []string) {
	ctx := context.Background()
	m.Logger.Info("Consumer...")
	for {
		if err := m.ConsumerGroup.Consume(ctx, topics, m); err != nil {
			m.Logger.Error("Consume Error", zap.Error(err))
			return
		}
	}
}

// ConsumeClaim push message
func (m *Manager) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		// push message
		if msg != nil {
			// 自动提交开启时，将所有的消息推送到 m.ConsumerMessages，不需要推送ConsumerGroupSession
			if m.option.autoSubmit {
				s.MarkMessage(msg, "done")
				m.ConsumerMessages <- msg
				return nil
			}

			// 手动提交逻辑
			m.Consumers <- &consumer{
				message: msg,
				session: s,
			}
		}
	}

	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (m *Manager) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (m *Manager) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (m *Manager) Close() {
	var err error
	if m.Producer != nil {
		err = m.Producer.Close()
		if err != nil {
			m.Logger.Error("Close Producer failed", zap.Error(err))
		}
	}

	if m.ConsumerGroup != nil {
		err = m.ConsumerGroup.Close()
		if err != nil {
			m.Logger.Error("Close ConsumerGroup failed", zap.Error(err))
		}
	}
}
