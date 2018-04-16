package common

import (
	"bytes"
	"database/sql"
	"errors"
	"math/big"
	"text/template"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/golang/glog"
	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	dbh *sql.DB

	// prepared statements
	updateKV          *sql.Stmt
	insertJob         *sql.Stmt
	selectJobs        *sql.Stmt
	stopReason        *sql.Stmt
	insertRec         *sql.Stmt
	insertClaim       *sql.Stmt
	setClaim          *sql.Stmt
	setClaimStatus    *sql.Stmt
	unclaimedReceipts *sql.Stmt
}

type DBJob struct {
	ID          int64
	txID        string
	streamID    string
	cost        int64
	options     string
	broadcaster ethcommon.Address
	transcoder  ethcommon.Address
	startBlock  int64
	endBlock    int64
	stopReason  string
}

var LivepeerDBVersion = 1

var ErrDBTooNew = errors.New("DB Too New")

var schema = `
	CREATE TABLE IF NOT EXISTS kv (
		key STRING PRIMARY KEY,
		value STRING,
		updatedAt STRING DEFAULT CURRENT_TIMESTAMP
	);
	INSERT OR IGNORE INTO kv(key, value) VALUES('dbVersion', '{{ . }}');
	INSERT OR IGNORE INTO kv(key, value) VALUES('lastBlock', '0');

	CREATE TABLE IF NOT EXISTS jobs (
		id INTEGER PRIMARY KEY,
		recordedAt STRING DEFAULT CURRENT_TIMESTAMP,
		txID STRING,
		streamID STRING,
		segmentCost INTEGER,
		transcodeOptions STRING,
		broadcaster STRING,
		transcoder STRING,
		startBlock INTEGER,
		endBlock INTEGER,
		stopReason STRING DEFAULT NULL,
		stoppedAt STRING DEFAULT NULL
	);

	CREATE TABLE IF NOT EXISTS receipts (
		jobID INTEGER NOT NULL,
		claimID INTEGER,
		seqNo INTEGER NOT NULL,
		bcastHash STRING,
		bcastSig STRING,
		transcodedHash STRING,
		transcodeStartedAt STRING,
		transcodeEndedAt STRING,
		errorMsg STRING DEFAULT NULL,
		PRIMARY KEY(jobID, seqNo),
		FOREIGN KEY(jobID) REFERENCES jobs(id),
		FOREIGN KEY(claimID) REFERENCES claims(id)
	);

	CREATE TABLE IF NOT EXISTS claims (
		id INTEGER PRIMARY KEY,
		claimRoot STRING,
		claimBlock INTEGER,
		claimedAt STRING DEFAULT CURRENT_TIMESTAMP,
		updatedAt STRING,
		status STRING DEFAULT 'Submitted'
	);
`

func NewDBJob(id *big.Int, txID string, streamID string,
	segmentCost *big.Int, transcodeOptions string,
	broadcaster ethcommon.Address, transcoder ethcommon.Address,
	startBlock *big.Int, endBlock *big.Int) *DBJob {
	return &DBJob{
		ID: id.Int64(), txID: txID, streamID: streamID, options: transcodeOptions,
		cost: segmentCost.Int64(), broadcaster: broadcaster, transcoder: transcoder,
		startBlock: startBlock.Int64(), endBlock: endBlock.Int64(),
	}
}

func InitDB(dbPath string) (*DB, error) {
	// XXX need a way to ensure (via unit tests?) that all DB{} fields are
	// properly closed / cleaned up in the case of an error
	d := DB{}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		glog.Error("Unable to open DB ", dbPath, err)
		return nil, err
	}
	d.dbh = db
	schemaBuf := new(bytes.Buffer)
	tmpl := template.Must(template.New("schema").Parse(schema))
	tmpl.Execute(schemaBuf, LivepeerDBVersion)
	_, err = db.Exec(schemaBuf.String())
	if err != nil {
		glog.Error("Error initializing schema ", err)
		d.Close()
		return nil, err
	}

	// updateKV prepared statement
	stmt, err := db.Prepare("UPDATE kv SET value=?, updatedAt = datetime() WHERE key=?")
	if err != nil {
		glog.Error("Unable to prepare updatekv stmt ", err)
		d.Close()
		return nil, err
	}
	d.updateKV = stmt

	// insertJob prepared statement
	stmt, err = db.Prepare("INSERT INTO jobs(id, txID, streamID, segmentCost, transcodeOptions, broadcaster, transcoder, startBlock, endBlock) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		glog.Error("Unable to prepare insertjob stmt ", err)
		d.Close()
		return nil, err
	}
	d.insertJob = stmt

	// select all jobs since
	stmt, err = db.Prepare("SELECT id, txID, streamID, segmentCost, transcodeOptions, broadcaster, transcoder, startBlock, endBlock FROM jobs WHERE endBlock > ? AND stopReason IS NULL")
	if err != nil {
		glog.Error("Unable to prepare selectjob stmt ", err)
		d.Close()
		return nil, err
	}
	d.selectJobs = stmt

	// set reason for stopping a job
	stmt, err = db.Prepare("UPDATE jobs SET stopReason=?, stoppedAt=datetime() WHERE id=?")
	if err != nil {
		glog.Error("Unable to prepare stop reason statement ", err)
		d.Close()
		return nil, err
	}
	d.stopReason = stmt

	// Insert receipt prepared statement
	stmt, err = db.Prepare("INSERT INTO receipts(jobID, seqNo, bcastHash, bcastSig, transcodedHash, transcodeStartedAt, transcodeEndedAt) VALUES(?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		glog.Error("Unable to prepare insert segment ", err)
		d.Close()
		return nil, err
	}
	d.insertRec = stmt

	// Claim related prepared statements
	stmt, err = db.Prepare("INSERT INTO claims(id, claimRoot) VALUES(?, ?)")
	if err != nil {
		glog.Error("Unable to prepare insert claims ", err)
		d.Close()
		return nil, err
	}
	d.insertClaim = stmt
	stmt, err = db.Prepare("UPDATE receipts SET claimID = ? WHERE seqNo BETWEEN ? AND ?")
	if err != nil {
		glog.Error("Unable to prepare setclaimid ", err)
		d.Close()
		return nil, err
	}
	d.setClaim = stmt

	stmt, err = db.Prepare("UPDATE claims SET status = ?, updatedAt = datetime() WHERE id = ?")
	if err != nil {
		glog.Error("Unable to prepare  setclaimstatus ", err)
		d.Close()
		return nil, err
	}
	d.setClaimStatus = stmt

	// Check for correct DB version and upgrade if needed
	var dbVersion int
	row := db.QueryRow("SELECT value FROM kv WHERE key = 'dbVersion'")
	err = row.Scan(&dbVersion)
	if err != nil {
		glog.Error("Unable to fetch DB version ", err)
		d.Close()
		return nil, err
	}
	if dbVersion > LivepeerDBVersion {
		glog.Error("Database too new")
		d.Close()
		return nil, ErrDBTooNew
	} else if dbVersion < LivepeerDBVersion {
		// Upgrade stepwise up to the correct version using the migration
		// procedure for each version
	} else if dbVersion == LivepeerDBVersion {
		// all good
	}

	glog.V(DEBUG).Info("Initialized DB node")
	return &d, nil
}

func (db *DB) Close() {
	glog.V(DEBUG).Info("Closing DB")
	if db.updateKV != nil {
		db.updateKV.Close()
	}
	if db.insertJob != nil {
		db.insertJob.Close()
	}
	if db.selectJobs != nil {
		db.selectJobs.Close()
	}
	if db.stopReason != nil {
		db.stopReason.Close()
	}
	if db.insertRec != nil {
		db.insertRec.Close()
	}
	if db.insertClaim != nil {
		db.insertClaim.Close()
	}
	if db.setClaim != nil {
		db.setClaim.Close()
	}
	if db.setClaimStatus != nil {
		db.setClaimStatus.Close()
	}
	if db.dbh != nil {
		db.dbh.Close()
	}
}

func (db *DB) SetLastSeenBlock(block *big.Int) error {
	if db == nil {
		return nil
	}
	glog.V(DEBUG).Info("db: Setting LastSeenBlock to ", block)
	_, err := db.updateKV.Exec(block.String(), "lastBlock")
	if err != nil {
		glog.Error("db: Got err in updating block ", err)
		return err
	}
	return err
}

func (db *DB) InsertJob(job *DBJob) error {
	if db == nil {
		return nil
	}
	glog.V(DEBUG).Info("db: Inserting job ", job.ID)
	_, err := db.insertJob.Exec(job.ID, job.txID, job.streamID,
		job.cost, job.options,
		job.broadcaster.String(), job.transcoder.String(),
		job.startBlock, job.endBlock)
	if err != nil {
		glog.Error("db: Unable to insert job ", err)
	}
	return err
}

func (db *DB) ActiveJobs(since *big.Int) ([]*DBJob, error) {
	if db == nil {
		return []*DBJob{}, nil
	}
	glog.V(DEBUG).Info("db: Querying active jobs since ", since)
	rows, err := db.selectJobs.Query(since.Int64())
	if err != nil {
		glog.Error("db: Unable to select jobs ", err)
		return nil, err
	}
	defer rows.Close()
	jobs := []*DBJob{}
	for rows.Next() {
		var job DBJob
		var transcoder string
		var broadcaster string
		if err := rows.Scan(&job.ID, &job.txID, &job.streamID, &job.cost, &job.options, &broadcaster, &transcoder, &job.startBlock, &job.endBlock); err != nil {
			glog.Error("db: Unable to fetch job ", err)
			continue
		}
		job.transcoder = ethcommon.HexToAddress(transcoder)
		job.broadcaster = ethcommon.HexToAddress(broadcaster)
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

func (db *DB) SetStopReason(id *big.Int, reason string) error {
	if db == nil {
		return nil
	}
	glog.V(DEBUG).Infof("db: Setting StopReason for %v to %v", id, reason)
	_, err := db.stopReason.Exec(reason, id.Int64())
	if err != nil {
		glog.Error("db: Error setting stop reason ", id, err)
		return err
	}
	return nil
}

func (db *DB) InsertReceipt(jobID *big.Int, seqNo int64,
	bcastHash []byte, bcastSig []byte, tcodeHash []byte,
	tcodeStartedAt time.Time, tcodeEndedAt time.Time) error {
	if db == nil {
		return nil
	}
	glog.V(DEBUG).Infof("db: Inserting receipt for %v - %v", jobID.String(), seqNo)
	time2str := func(t time.Time) string {
		return t.UTC().Format("2006-01-02 15:04:05")
	}
	_, err := db.stopReason.Exec(jobID.Int64(), seqNo,
		ethcommon.ToHex(bcastHash), ethcommon.ToHex(bcastSig),
		ethcommon.ToHex(tcodeHash),
		time2str(tcodeStartedAt), time2str(tcodeEndedAt))
	if err != nil {
		glog.Error("db: Error inserting segment ", jobID, err)
		return err
	}
	return nil
}

func (db *DB) InsertClaim(claimID *big.Int, segRange [2]*big.Int, root [32]byte) error {
	if db == nil {
		return nil
	}
	tx, err := db.dbh.Begin()
	if err != nil {
		glog.Error("Unable to begin tx ", err)
		return err
	}
	insert := tx.Stmt(db.insertClaim)
	update := tx.Stmt(db.setClaim)
	res, err := insert.Exec(claimID.Int64(), ethcommon.ToHex(root[:]))
	if err != nil {
		glog.Error("Unable to insert claim ", err)
		tx.Rollback()
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		glog.Error("Unable to get last ID ", err)
		tx.Rollback()
		return err
	}
	_, err = update.Exec(id, segRange[0].Int64, segRange[1].Int64)
	if err != nil {
		glog.Error("Unable to update segments with claims ", err)
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		glog.Error("Unable to commit tx ", err)
		tx.Rollback()
		return err
	}
	return nil
}

func (db *DB) SetClaimStatus(id *big.Int, status string) error {
	if db == nil {
		return nil
	}
	glog.V(DEBUG).Infof("db: Setting ClaimStatus for %v to %v", id, status)
	_, err := db.setClaimStatus.Exec(status, id.Int64())
	if err != nil {
		glog.Error("db: Error setting claim status ", id, err)
		return err
	}
	return nil
}
