package txmsg

import (
	"fmt"
	"time"
)

// 本地消息表sql相关
var (
	TableName = "mq_messages"

	InsertSQL = fmt.Sprintf("insert into %s(content, topic, tag, status, delay, create_time) values(?, ?, ?, ?, ?, ?) ", TableName)

	SelectByID = fmt.Sprintf("select id, content, topic, tag, status, create_time, delay from %s where id=? ", TableName)

	// 获取最小等待事务
	SelectMinIdOfWaitingSQL = fmt.Sprintf("select min(id) from %s where status=? ", TableName)

	// 更改事务消息状态
	UpdateStatusSQL = fmt.Sprintf("update %s set status=? where id=? ", TableName)

	// 获取 等待 事务消息列表
	SelectWaitingMsgSQL = fmt.Sprintf("select id, content, topic, tag, status, create_time, delay from %s where status=? and create >= ? order by id limit ?", TableName)

	// 获取指定主题下的 等待事务消息列表
	SelectWaitingMsgWithTopicsSQL = fmt.Sprintf("select id, content, topic, tag, status, create_time, delay from %s where status=? and create >= ? and topic in ", TableName)
	// 删除sql
	DeleteMsgSQL = fmt.Sprintf("delete from %s where status=? and create_time <= ? limit ? ", TableName)

	// delay列检测
	CheckDelayColumnSQL = fmt.Sprintf("select count(column_name) from information_schema.columns where table_schema = DATABASE() and table_name = '%s' and column_name = 'delay'", TableName)

)

const (
	DefaultTransKey = "/txmsg/defaultTransKey"

	MqProducerName = "transactionMsgProducer"
)


const (
	WorkNum = 10 // 事务消息工作协程数目

	DeleteTimePeriod = 180  // 每180s扫表一次，删除历史消息

	DeleteMsgOneTimeNum = 200  // 一次性删除200条消息

	ScheduleWorkNum = 6

	SendMsgTimeOut = 600  // ms

	MaxDelay = 90 * 24 * 60 * 60  // s

	ScheduleScanTimePeriod = 120  // 120s扫描一次，待发送的继续发送

	StatsTimePeriod = 120

	HistoryMsgStoreTime = 3

	DayTimeDiff = HistoryMsgStoreTime * time.Hour * 24

	LimitNum = 50

	MaxDealNumOneTime = 2000

	HoldLockTime = 60

	MaxDealTime = 6

)

var TimeoutData  = [6]int{0, 5, 10, 25, 50, 100}

const (
	SVC_CREATE = 0  // 服务创建态

	SVC_RUNNING = 1 // 服务运行态

	SVC_CLOSE = 2 	// 服务关闭态

	SCV_FAILED = 3 	// 服务失败态
)

const (
	MSG_STATUS_WAITING = 1 // 事务消息-等待

	MSG_STATUS_SEND = 2 // 事务消息-发送
)