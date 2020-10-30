package txmsg

import (
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"time"
)

type TxMsgClient struct {
	config *Config
	Producer rocketmq.Producer // 生产者
	MsgStorage *MsgStorage
	MsgProcessor *MsgProcessor
	state atomic.Value
	dbKey string
	db *sqlx.DB
}


func NewTxMsgClient(db *sqlx.DB, dbKey string, topicLists []string, cfg *Config) (*TxMsgClient, error) {
	msgStorage := NewMsgStorage(db, topicLists)
	mqProducer, err := rocketmq.NewProducer(producer.WithNameServer(cfg.RocketMQAddrs), producer.WithRetry(3),
		producer.WithGroupName(MqProducerName), producer.WithSendMsgTimeout(SendMsgTimeOut * time.Millisecond))

	if err != nil {
		return nil, err
	}

	msgProcessor := NewMsgProcessor(dbKey, mqProducer, msgStorage, cfg)

	cli := &TxMsgClient{
		config: cfg,
		Producer: mqProducer,
		MsgStorage: msgStorage,
		MsgProcessor: msgProcessor,
		db: db,
		dbKey: dbKey,
	}

	cli.state.Store(SVC_CREATE)

	return cli, nil
}

func (p *TxMsgClient) Init() error {
	if p.state.Load().(int) == SVC_RUNNING {
		return nil
	}

	err :=p.Producer.Start()
	if err != nil {
		log.Println(err)
		return err
	}

	err = p.MsgProcessor.Init()
	if err != nil {
		log.Println(err)
		return err
	}

	err = p.MsgStorage.Init()
	if err != nil {
		log.Println(err)
		return err
	}

	p.state.Store(SVC_RUNNING)
	return nil
}

func (p *TxMsgClient) Close() error {
	p.MsgProcessor.Close()
	p.MsgStorage.Close()
	err := p.Producer.Shutdown()
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (p *TxMsgClient) SendMsg(content, topic, tag string, delay int64) (int64, error) {
	if p.state.Load().(int) != SVC_RUNNING {
		return 0, errors.New("service is not running")
	}

	if content == "" || topic == "" {
		return 0, errors.New("the topic or content is null or empty")
	}

	if !p.MsgStorage.IsInTopicLists(topic) {
		return 0, errors.New("the topic is not in list")
	}

	if delay < 0 || delay > MaxDelay {
		return 0, errors.New("delay is less than min delay or greater than max delay")
	}

	id, err := p.MsgStorage.InsertMsg( content, topic, tag, delay)
	if err != nil {
		log.Println(err)
		return 0, err
	}

	msg := NewMsg(id)

	p.MsgProcessor.PutMsg(msg)

	return id, nil
}