package txmsg

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"time"
)

type MsgProcessor struct {
	Producer rocketmq.Producer // 生产者
	MsgStorage *MsgStorage  // 事务消息数据访层
	MsgQueue *MsgPriorityQueue // 事务操作消息队列
	TimeWheel *MsgPriorityQueue // 时间轮投递
	cfg *Config
	state atomic.Value
	holdLock atomic.Value  // 是否持有锁
	ectdCli *clientv3.Client
}

func NewMsgProcessor(producer rocketmq.Producer, storage *MsgStorage, cfg *Config) *MsgProcessor {
	processor := &MsgProcessor{
		Producer: producer,
		MsgStorage: storage,
		cfg: cfg,
	}

	processor.state.Store(SVC_CREATE)
	processor.holdLock.Store(false)

	processor.MsgQueue = NewMsgPriorityQueue(func(msg1, msg2 *Msg) bool {
		diff := msg1.CreateTime - msg2.CreateTime
		if diff > 0 {
			return true
		}

		return false
	})

	processor.TimeWheel = NewMsgPriorityQueue(func(msg1, msg2 *Msg) bool {
		diff := msg1.NextExpireTime - msg2.NextExpireTime
		if diff > 0 {
			return true
		}

		return false
	})



	return processor
}

func (p *MsgProcessor) Init() error {
	if len(p.cfg.EtcdHosts) == 0 {
		return errors.New("etcd地址不能为空")
	}

	var err error
	p.ectdCli, err = clientv3.New(clientv3.Config{Endpoints: p.cfg.EtcdHosts})
	if err != nil {
		return nil
	}

	if p.state.Load().(int) == SVC_RUNNING {
		return nil
	}

	p.state.Store(SVC_RUNNING)


	go p.scanMsgTask()
	go p.cleanMsgTask()
	go p.keepLockTask()
	go p.deliveryTask()

	return nil
}

func (p *MsgProcessor) Close() {
	p.state.Store(SVC_CLOSE)
	p.ectdCli.Close()
}

func (p *MsgProcessor) PutMsg(msg *Msg) {
	p.MsgQueue.Push(msg)
}

func (p *MsgProcessor) buildMQMessage(msgInfo *MsgInfo) *primitive.Message {
	message := &primitive.Message{
		Topic: msgInfo.Topic,
		Body: []byte(msgInfo.Content),
	}

	idStr := fmt.Sprintf("%d", msgInfo.Id)
	header := &map[string]string{
		"topic": msgInfo.Topic,
		"tag": msgInfo.Tag,
		"id": idStr,
		"createTime": fmt.Sprintf("%d", GetMilliSecond(time.Now())),
	}

	bs, _ := json.Marshal(header)

	message.WithKeys([]string{idStr}).WithTag(msgInfo.Tag).WithProperty("MQHeader", string(bs))

	return message
}

// 补漏协程：扫描最近10分钟未提交事务消息，防止各种场景的消息丢失
func (p *MsgProcessor) scanMsgTask() {
	for {
		time.Sleep(ScheduleScanTimePeriod * time.Second)

		if p.state.Load().(int) != SVC_RUNNING {
			continue
		}

		p.MsgStorage.TMDataSourceMap.Range(func(key, dbVal interface{}) bool {
			// 没有持有锁
			if !p.holdLock.Load().(bool) {
				return false
			}

			db := dbVal.(*SqlxDBWrap)

			num := LimitNum
			cnt := 0

			for num == LimitNum && cnt < MaxDealNumOneTime {
				msgInfoList, err := p.MsgStorage.GetWaitingMsg(db, LimitNum)
				if err != nil {
					break
				}

				num = len(msgInfoList)
				cnt += num


				for i := 0; i < len(msgInfoList); i++ {
					mqMsg := p.buildMQMessage(msgInfoList[i])
					result, err := p.Producer.SendSync(context.Background(), mqMsg)
					if err == nil && result.Status == primitive.SendOK {
						err = p.MsgStorage.UpdateMsgStatus(db, msgInfoList[i].Id)
						if err != nil {
							log.Println(err)
						}
					}
				}
			}

			return true
		})
	}
}

func (p *MsgProcessor) keepLockTask() {

	// 延迟10s竞争锁
	time.Sleep(10*time.Second)
	for p.state.Load().(int) == SVC_RUNNING {

		// 适用CP锁，选择业务处理的主节点
		session, err := concurrency.NewSession(p.ectdCli, concurrency.WithTTL(HoldLockTime))
		if err != nil {
			log.Println(err)
			continue
		}

		mutex := concurrency.NewMutex(session, DefaultTransKey)

		err = mutex.Lock(context.Background())
		if err == nil {
			p.holdLock.Store(true)
		} else {
			p.holdLock.Store(false)
		}

		log.Println("its keep lock")

		// 持有锁 holdlockTime
		time.Sleep(HoldLockTime * time.Second)
		p.holdLock.Store(false)

		mutex.Unlock(context.Background())
		session.Close()
	}
}

// 删除3天前的数据
func (p *MsgProcessor) cleanMsgTask() {
	for p.state.Load().(int) == SVC_RUNNING {
		time.Sleep(DeleteTimePeriod * time.Second)

		p.MsgStorage.TMDataSourceMap.Range(func(key, dbVal interface{}) bool {

			if !p.holdLock.Load().(bool) {
				return false
			}

			db := dbVal.(*SqlxDBWrap)
			cnt := int64(0)
			num := int64(DeleteMsgOneTimeNum)
			for num == DeleteMsgOneTimeNum && cnt < MaxDealNumOneTime {
				var err error
				num, err = p.MsgStorage.DeleteSendedMsg(db, DeleteMsgOneTimeNum)
				if err != nil {
					continue
				}
				cnt += num
			}

			return true
		})
	}
}


// 异步投递任务
func (p *MsgProcessor) deliveryTask() {
	for p.state.Load().(int) == SVC_RUNNING {
		msg := p.MsgQueue.Pop()
		if msg == nil {
			continue
		}

		msg.HaveDealedTimes += 1

		msgInfo, err := p.MsgStorage.GetMsgById(msg)
		if err != nil {
			log.Println(err)
			continue
		}

		// 如果数据库没有此消息
		if msg == nil {
			continue
		}

		mqMsg := p.buildMQMessage(msgInfo)

		result, err := p.Producer.SendSync(context.Background(), mqMsg)
		if err != nil {
			log.Println(err)
		}

		if result.Status == primitive.SendOK {
			_ = p.MsgStorage.UpdateSendMsg(msg)
		} else {
			if msg.HaveDealedTimes < MaxDealTime {
				msg.NextExpireTime = GetMilliSecond(time.Now().Add(time.Duration(msg.HaveDealedTimes) * time.Second))
				p.TimeWheel.Push(msg)
			}
		}

	}
}

func (p *MsgProcessor) timeWheelTime() {
	for p.state.Load().(int) == SVC_RUNNING {
		msg := p.TimeWheel.Pop()
		if msg == nil {
			continue
		}

		curTs := GetMilliSecond(time.Now())

		if msg.NextExpireTime > curTs {
			time.Sleep(time.Duration(msg.NextExpireTime-curTs) * time.Millisecond)
		}

		p.MsgQueue.Push(msg)
	}
}