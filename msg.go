package txmsg

import (
	"time"
)

// 事务操作实体
type Msg struct {
	Id              int64  // 主键
	Url             string // 数据源
	HaveDealedTimes int    // 已经处理次数
	CreateTime      int64  // 创建时间
	NextExpireTime  int64  // 下次超时时间
}

func NewMsg(id int64, url string) *Msg {
	return &Msg{
		Id:         id,
		Url:        url,
		CreateTime: GetMilliSecond(time.Now()),
	}
}

// 事务消息实体
type MsgInfo struct {
	Id         int64  `db:"id" json:"id"`         // 主键
	Content    string `db:"content" json:"content"`    // 事务消息
	Topic      string `db:"topic" json:"topic"`      // 主题
	Tag        string `db:"tag" json:"tag"`        // 标签
	Status     int    `db:"status" json:"status"`     // 状态
	CreateTime int64  `db:"create_time" json:"createTime"` // 创建时间
	Delay      int    `db:"delay" json:"delay"`      // 延迟时间（单位s）
}

// 数据源信息对象
type TxMsgDataSource struct {
	Username string // 用户名
	Password string // 密码
	Host     string // 地址
	Port     int64  // 端口
	DBName   string // 数据库名称
}



func NewTxMsgDataSource(username, password, host, dbName string, port int64) *TxMsgDataSource {
	txDataSource := &TxMsgDataSource{
		Username: username,
		Password: password,
		Host:     host,
		DBName:   dbName,
		Port:     port,
	}

	return txDataSource
}
