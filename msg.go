package txmsg

import (
	"time"
)

// 事务操作实体
type Msg struct {
	Id              int64  // 主键
	HaveDealedTimes int    // 已经处理次数
	CreateTime      int64  // 创建时间
	NextExpireTime  int64  // 下次超时时间
}

func NewMsg(id int64) *Msg {
	return &Msg{
		Id:         id,
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

