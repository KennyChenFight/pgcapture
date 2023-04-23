package source

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/rueian/pgcapture/pkg/cursor"
	"github.com/rueian/pgcapture/pkg/decode"
	"github.com/rueian/pgcapture/pkg/sql"
	"github.com/sirupsen/logrus"
)

type PGXSource struct {
	BaseSource

	SetupConnStr string
	ReplConnStr  string
	ReplSlot     string
	CreateSlot   bool
	StartLSN     string

	setupConn      *pgx.Conn
	replConn       *pgconn.PgConn
	schema         *decode.PGXSchemaLoader
	decoder        *decode.PGLogicalDecoder
	nextReportTime time.Time
	ackLsn         uint64
	txCounter      uint64
	log            *logrus.Entry
	first          bool
	currentLsn     uint64
	currentSeq     uint32
}

func (p *PGXSource) TxCounter() uint64 {
	return atomic.LoadUint64(&p.txCounter)
}

func (p *PGXSource) Capture(cp cursor.Checkpoint) (changes chan Change, err error) {
	defer func() {
		if err != nil {
			p.cleanup()
		}
	}()

	ctx := context.Background()
	p.setupConn, err = pgx.Connect(ctx, p.SetupConnStr)
	if err != nil {
		return nil, err
	}

	var sv string
	// 撈取 server version 是為了給 pglogical plugin 參數做使用
	if err = p.setupConn.QueryRow(ctx, sql.ServerVersionNum).Scan(&sv); err != nil {
		return nil, err
	}

	svn, err := strconv.ParseInt(sv, 10, 64)
	if err != nil {
		return nil, err
	}

	// install pgcapture extension
	if _, err = p.setupConn.Exec(ctx, sql.InstallExtension); err != nil {
		return nil, err
	}

	// cache 特定 schema 下的所有 table 的全部欄位型態 oid 等資料
	p.schema = decode.NewPGXSchemaLoader(p.setupConn)
	if err = p.schema.RefreshType(); err != nil {
		return nil, err
	}

	// 設定 logical decode plugin -> 目前只有 pglogical_output
	p.decoder = decode.NewPGLogicalDecoder(p.schema)

	if p.CreateSlot {
		// create logical replication slot
		if _, err = p.setupConn.Exec(ctx, sql.CreateLogicalSlot, p.ReplSlot, decode.OutputPlugin); err != nil {
			if pge, ok := err.(*pgconn.PgError); !ok || pge.Code != "42710" {
				return nil, err
			}
		}
	}

	p.replConn, err = pgconn.Connect(context.Background(), p.ReplConnStr)
	if err != nil {
		return nil, err
	}

	// 確認 replica 與 primary 的關係
	ident, err := pglogrepl.IdentifySystem(context.Background(), p.replConn)
	if err != nil {
		return nil, err
	}

	p.log = logrus.WithFields(logrus.Fields{"From": "PGXSource"})
	p.log.WithFields(logrus.Fields{
		"SystemID": ident.SystemID,
		"Timeline": ident.Timeline,
		"XLogPos":  int64(ident.XLogPos),
		"DBName":   ident.DBName,
	}).Info("retrieved current info of source database")

	// 從 sink 最新的 LSN 開始從 primary 撈取資料
	if cp.LSN != 0 {
		p.currentLsn = cp.LSN
		p.currentSeq = cp.Seq
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  p.currentLsn,
		}).Info("start logical replication from requested position")
	} else {
		// 設定指令的 LSN
		if p.StartLSN != "" {
			startLsn, err := pglogrepl.ParseLSN(p.StartLSN)
			if err != nil {
				return nil, err
			}
			p.currentLsn = uint64(startLsn)
		} else {
			// 預設使用 source 當前最新的 flush LSN (logical replication slot)
			p.currentLsn = uint64(ident.XLogPos)
		}
		p.currentSeq = 0
		p.log.WithFields(logrus.Fields{
			"ReplSlot": p.ReplSlot,
			"FromLSN":  p.currentLsn,
		}).Info("start logical replication from the latest position")
	}
	// 儲存當前 source 使用的 LSN
	p.Commit(cursor.Checkpoint{LSN: p.currentLsn})
	if err = pglogrepl.StartReplication(
		context.Background(),
		p.replConn,
		p.ReplSlot,
		pglogrepl.LSN(p.currentLsn),
		pglogrepl.StartReplicationOptions{PluginArgs: decode.PGLogicalParam(svn)},
	); err != nil {
		return nil, err
	}

	return p.BaseSource.capture(p.fetching, p.cleanup)
}

func (p *PGXSource) fetching(ctx context.Context) (change Change, err error) {
	// 每隔五秒鐘要對 source 進行 report LSN
	// 對於 PG 而言，收到對應的 report LSN，代表 replica 已經不需要 report LSN 之前的資料了
	if time.Now().After(p.nextReportTime) {
		if err = p.reportLSN(ctx); err != nil {
			return change, err
		}
		p.nextReportTime = time.Now().Add(5 * time.Second)
	}
	msg, err := p.replConn.ReceiveMessage(ctx)
	if err != nil {
		return change, err
	}
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		// 如果 replica 太久沒通知 primary，primary 會透過這個 message 要求 replica 回覆
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			var pkm pglogrepl.PrimaryKeepaliveMessage
			if pkm, err = pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:]); err == nil && pkm.ReplyRequested {
				p.nextReportTime = time.Time{}
			}
		// 收到 wal data
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return change, err
			}
			// decode wal data，並將資料轉換成對應的 Change type
			m, err := p.decoder.Decode(xld.WALData)
			if m == nil || err != nil {
				return change, err
			}
			// change 是 insert, update, delete 類型
			if msg := m.GetChange(); msg != nil {
				// ignore pgcapture schema 下的 sources table
				// 原因在於 sources 是給下游的 PG 使用的
				if decode.Ignore(msg) {
					return change, nil
				} else if decode.IsDDL(msg) {
					// 如果是 DDL，需要更新 schema
					if err = p.schema.RefreshType(); err != nil {
						return change, err
					}
				}
				// 儲存 seq 是為了要當 source restart 的時候可以去比對正確的 LSN
				// 因為 begin 跟 commit 之間的所有 LSN 會一樣，中間的差異是 seq
				p.currentSeq++
			} else if b := m.GetBegin(); b != nil {
				p.currentLsn = b.FinalLsn
				p.currentSeq = 0
			} else if c := m.GetCommit(); c != nil {
				p.currentLsn = c.CommitLsn
				p.currentSeq++
			}
			change = Change{
				Checkpoint: cursor.Checkpoint{LSN: p.currentLsn, Seq: p.currentSeq},
				Message:    m,
			}
			if !p.first {
				p.log.WithFields(logrus.Fields{
					"MessageLSN": change.Checkpoint.LSN,
					"Message":    m.String(),
				}).Info("retrieved the first message from postgres")
				p.first = true
			}
		}
	default:
		err = errors.New("unexpected message")
	}
	return change, err
}

func (p *PGXSource) Commit(cp cursor.Checkpoint) {
	if cp.LSN != 0 {
		atomic.StoreUint64(&p.ackLsn, cp.LSN)
		atomic.AddUint64(&p.txCounter, 1)
	}
}

func (p *PGXSource) Requeue(cp cursor.Checkpoint, reason string) {
}

func (p *PGXSource) committedLSN() (lsn pglogrepl.LSN) {
	return pglogrepl.LSN(atomic.LoadUint64(&p.ackLsn))
}

// 通知 primary replica 已經收到的 LSN，則 primary 可以清空該 LSN 之前的資料
func (p *PGXSource) reportLSN(ctx context.Context) error {
	if committed := p.committedLSN(); committed != 0 {
		return pglogrepl.SendStandbyStatusUpdate(ctx, p.replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: committed})
	}
	return nil
}

func (p *PGXSource) cleanup() {
	ctx := context.Background()
	if p.setupConn != nil {
		p.setupConn.Close(ctx)
	}
	if p.replConn != nil {
		p.reportLSN(ctx)
		p.replConn.Close(ctx)
	}
}
