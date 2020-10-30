package txmsg

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"log"
	"sync"
	"time"
)

type MsgStorage struct {
	TopicLists []string // 主题列表
	TMDataSources []*TxMsgDataSource // 数据源
	TMDataSourceMap *sync.Map
}

func NewMsgStorage(tmDataSources []*TxMsgDataSource, topicLists []string) *MsgStorage {
	return &MsgStorage{
		TopicLists: topicLists,
		TMDataSources: tmDataSources,
		TMDataSourceMap: new(sync.Map),
	}
}

func (p *MsgStorage) Init() error {

	for i := 0; i < len(p.TMDataSources); i++ {
		sourceInfo := p.TMDataSources[i]
		dbSrc := fmt.Sprintf("%s:%s@(%s:%d)/%s?parseTime=true&loc=Local", sourceInfo.Username,
			sourceInfo.Password, sourceInfo.Host, sourceInfo.Port, sourceInfo.DBName)
		db, err := sqlx.Connect("mysql", dbSrc)


		if err != nil {
			log.Println(err.Error())
			return err
		}

		err = db.Ping()
		if err != nil {
			log.Println(err)
			return err
		}

		dbWrap := NewSqlxDBWrap(db, sourceInfo.DBName, sourceInfo.Host, sourceInfo.Port)
		err = p.checkDelayColumn(dbWrap)
		if err != nil {
			return err
		}



		p.TMDataSourceMap.Store(dbWrap.Url, dbWrap)
	}

	return nil
}

func (p *MsgStorage) checkDelayColumn(db *SqlxDBWrap) error {

	row := db.QueryRowx(CheckDelayColumnSQL)

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
	p.TMDataSourceMap.Range(func (key, db interface{}) bool {

		_ = db.(*SqlxDBWrap).Close()

		return true
	})
}

// 插入消息用的是业务方连接，由业务方管理
func (p *MsgStorage) InsertMsg(db *SqlxDBWrap, content, topic, tag string, delay int64) (int64, error) {

	result, err := db.Exec(InsertSQL, content, topic, tag, MSG_STATUS_WAITING, delay, GetMilliSecond(time.Now()))
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
	id := msg.Id
	dbVal, ok := p.TMDataSourceMap.Load(msg.Url)


	if !ok {
		return nil, errors.New("not find db msg")
	}

	db := dbVal.(*SqlxDBWrap)

	msgInfo := new(MsgInfo)
	err := db.Get(msgInfo, SelectByID, id)

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
	id := msg.Id
	dbVal, ok := p.TMDataSourceMap.Load(msg.Url)

	if !ok {
		return errors.New("not find db msg")
	}

	db := dbVal.(*SqlxDBWrap)

	_, err := db.Exec(UpdateStatusSQL, MSG_STATUS_SEND, id)
	return err
}


func (p *MsgStorage) UpdateMsgStatus(db *SqlxDBWrap, id int64) error {

	if db == nil {
		return errors.New("db can't nil")
	}

	_, err := db.Exec(UpdateStatusSQL, MSG_STATUS_SEND, id)
	return err
}

func (p *MsgStorage) GetMinIdOfWaitingMsg(db *SqlxDBWrap) (int64, error) {
	if db == nil {
		return 0, errors.New("db can't nil")
	}

	row := db.QueryRowx(SelectMinIdOfWaitingSQL, MSG_STATUS_WAITING)

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

func (p *MsgStorage) GetWaitingMsg(db *SqlxDBWrap, pageSize int) (result []*MsgInfo, err error) {

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
		row = db.QueryRowx(waitMsgSql, MSG_STATUS_WAITING)
	} else {
		row = db.QueryRowx(waitMsgSql,  params...)
	}

	err = row.Scan(result)
	if err == sql.ErrNoRows {
		err = nil
		return
	}


	return
}

func (p *MsgStorage) DeleteSendedMsg(db *SqlxDBWrap, limitNum int) (int64, error) {
	result, err := db.Exec(DeleteMsgSQL, MSG_STATUS_SEND, GetMilliSecondBeforeNow(DayTimeDiff), limitNum)

	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

