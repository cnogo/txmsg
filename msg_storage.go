package txmsg

import (
	"bytes"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"time"
)

type MsgStorage struct {
	TopicLists []string // 主题列表
	db *sqlx.DB
}

func NewMsgStorage(db *sqlx.DB, topicLists []string) *MsgStorage {
	return &MsgStorage{
		TopicLists: topicLists,
		db: db,
	}
}

func (p *MsgStorage) Init() error {

	err := p.checkDelayColumn()
	if err != nil {
		return err 
	}

	return nil
}

func (p *MsgStorage) checkDelayColumn() error {

	row := p.db.QueryRowx(CheckDelayColumnSQL)

	var count sql.NullInt64
	err := row.Scan(&count)
	if err != nil {
		return err
	}

	if count.Int64 < 1 {
		return errors.New("mq_messages table have not delay column")
	}

	return nil
}

func (p *MsgStorage) Close() {
}

// 插入消息用的是业务方连接，由业务方管理
func (p *MsgStorage) InsertMsg(content, topic, tag string, delay int64) (int64, error) {

	result, err := p.db.Exec(InsertSQL, content, topic, tag, MSG_STATUS_WAITING, delay, GetMilliSecond(time.Now()))
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

func (p *MsgStorage) IsInTopicLists(topic string) bool {
	if topic == "" {
		return false
	}

	for i := 0; i < len(p.TopicLists); i++ {
		if p.TopicLists[i] == topic {
			return true
		}
	}

	return false
}

func (p *MsgStorage) GetMsgById(msg *Msg) (*MsgInfo, error) {

	msgInfo := new(MsgInfo)
	err := p.db.Get(msgInfo, SelectByID, msg.Id)

	if err == sql.ErrNoRows {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return msgInfo, nil
}

// 更新事务消息为已经发送
func (p *MsgStorage) UpdateSendMsg(msg *Msg) error {

	_, err := p.db.Exec(UpdateStatusSQL, MSG_STATUS_SEND, msg.Id)
	return err
}


func (p *MsgStorage) UpdateMsgStatus(id int64) error {

	if p.db == nil {
		return errors.New("db can't nil")
	}

	_, err := p.db.Exec(UpdateStatusSQL, MSG_STATUS_SEND, id)
	return err
}

func (p *MsgStorage) GetMinIdOfWaitingMsg() (int64, error) {
	if p.db == nil {
		return 0, errors.New("db can't nil")
	}

	row := p.db.QueryRowx(SelectMinIdOfWaitingSQL, MSG_STATUS_WAITING)

	var id sql.NullInt64
	err := row.Scan(&id)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	return id.Int64, nil
}

func (p *MsgStorage) GetWaitingMsg(pageSize int) (result []*MsgInfo, err error) {
	if p.db == nil {
		return nil, errors.New("db can't nil")
	}

	waitMsgSql := SelectWaitingMsgSQL

	isWithTopic := false
	var params []interface{}
	if len(p.TopicLists) != 0 {

		params := append(params, MSG_STATUS_WAITING)
		buffer := bytes.NewBufferString(SelectWaitingMsgWithTopicsSQL)
		buffer.WriteString("(")

		for i := 0; i < len(p.TopicLists); i++ {
			if i < len(p.TopicLists) - 1 {
				buffer.WriteString(" ?, ")

			} else {
				buffer.WriteString(" ?")
			}

			params = append(params, p.TopicLists[i])
		}

		buffer.WriteString(" ) order by id limit ? ")
		waitMsgSql = buffer.String()
		isWithTopic = true
		params = append(params, pageSize)
	}

	var row *sqlx.Row

	if !isWithTopic {
		row = p.db.QueryRowx(waitMsgSql, MSG_STATUS_WAITING)
	} else {
		row = p.db.QueryRowx(waitMsgSql,  params...)
	}

	err = row.Scan(result)
	if err == sql.ErrNoRows {
		err = nil
		return
	}


	return
}

func (p *MsgStorage) DeleteSendedMsg(limitNum int) (int64, error) {
	result, err := p.db.Exec(DeleteMsgSQL, MSG_STATUS_SEND, GetMilliSecondBeforeNow(DayTimeDiff), limitNum)

	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

