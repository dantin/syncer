package main

import (
	"database/sql"
	"net/http"
	"strings"
	// For pprof
	_ "net/http/pprof"
	"strconv"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/siddontang/go-mysql/mysql"
)

var (
	binlogEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "binlog_events_total",
			Help:      "total number of binlog events",
		}, []string{"type"})

	binlogSkippedEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "binlog_skipped_events_total",
			Help:      "total number of skipped binlog events",
		}, []string{"type"})

	sqlJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "sql_jobs_total",
			Help:      "total number of sql jobs",
		}, []string{"type"})

	sqlRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "sql_retries_total",
			Help:      "total number of sql retryies",
		}, []string{"type"})

	binlogPos = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "binlog_pos",
			Help:      "current binlog pos",
		}, []string{"node"})

	binlogFile = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node"})

	binlogGTID = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "gtid",
			Help:      "current transaction id",
		}, []string{"node"})
)

func initStatusAndMetrics(addr string) {
	prometheus.MustRegister(binlogEventsTotal)
	prometheus.MustRegister(binlogSkippedEventsTotal)
	prometheus.MustRegister(sqlJobsTotal)
	prometheus.MustRegister(sqlRetriesTotal)
	prometheus.MustRegister(binlogPos)
	prometheus.MustRegister(binlogFile)
	prometheus.MustRegister(binlogGTID)

	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "application/text")
			text := utils.GetRawInfo("syncer")
			w.Write([]byte(text))
		})

		// HTTP path for prometheus.
		http.Handle("/metrics", prometheus.Handler())
		log.Infof("listening on %v for status and metrics report.", addr)
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
}

func getBinlogIndex(filename string) float64 {
	spt := strings.Split(filename, ".")
	if len(spt) == 1 {
		return 0
	}
	idxStr := spt[len(spt)-1]

	idx, err := strconv.ParseFloat(idxStr, 64)
	if err != nil {
		log.Warnf("[syncer] parse binlog index %s, error %s", filename, err)
		return 0
	}
	return idx
}

func masterGTIDGauge(gtidSet GTIDSet, db *sql.DB) {
	uuid, err := getServerUUID(db)
	if err != nil {
		log.Errorf("get server_uuid error %s", err)
		return
	}

	sets := make(map[string]*mysql.UUIDSet)

	if gtidSet.contain(uuid) {
		sets[uuid] = gtidSet.get(uuid)
	} else {
		sets = gtidSet.all()
	}

	for _, uuidSet := range sets {
		length := uuidSet.Intervals.Len()
		maxInterval := uuidSet.Intervals[length-1]
		// Why stop - 1? See github.com/siddontang/go-mysql parseInterval for more details.
		stop := maxInterval.Stop - 1
		binlogGTID.WithLabelValues("master").Set(float64(stop))
	}
}
