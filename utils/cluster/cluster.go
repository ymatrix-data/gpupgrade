package cluster

/*
 * This file contains structs and functions related to interacting
 * with files and directories, both locally and remotely over SSH.
 *
 * It was originally copied from gp-common-go-libs and will, hopefully, be
 * slowly merged into and subsumed by the upgrade Cluster object.
 */

import (
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type Cluster struct {
	ContentIDs []int
	Segments   map[int]SegConfig
}

type SegConfig struct {
	DbID      int
	ContentID int
	Port      int
	Hostname  string
	DataDir   string
}

/*
 * Base cluster functions
 */

func NewCluster(segConfigs []SegConfig) *Cluster {
	cluster := Cluster{}
	cluster.Segments = make(map[int]SegConfig, len(segConfigs))
	for _, seg := range segConfigs {
		cluster.ContentIDs = append(cluster.ContentIDs, seg.ContentID)
		cluster.Segments[seg.ContentID] = seg
	}
	return &cluster
}

func (cluster *Cluster) GetContentList() []int {
	return cluster.ContentIDs
}

func (cluster *Cluster) GetDbidForContent(contentID int) int {
	return cluster.Segments[contentID].DbID
}

func (cluster *Cluster) GetPortForContent(contentID int) int {
	return cluster.Segments[contentID].Port
}

func (cluster *Cluster) GetHostForContent(contentID int) string {
	return cluster.Segments[contentID].Hostname
}

func (cluster *Cluster) GetDirForContent(contentID int) string {
	return cluster.Segments[contentID].DataDir
}

/*
 * Helper functions
 */

func GetSegmentConfiguration(connection *dbconn.DBConn) ([]SegConfig, error) {
	query := ""
	if connection.Version.Before("6") {
		query = `
SELECT
	s.dbid,
	s.content as contentid,
	s.port,
	s.hostname,
	e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'p' AND f.fsname = 'pg_system'
ORDER BY s.content;`
	} else {
		query = `
SELECT
	dbid,
	content as contentid,
	port,
	hostname,
	datadir
FROM gp_segment_configuration
WHERE role = 'p'
ORDER BY content;`
	}

	results := make([]SegConfig, 0)
	err := connection.Select(&results, query)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func MustGetSegmentConfiguration(connection *dbconn.DBConn) []SegConfig {
	segConfigs, err := GetSegmentConfiguration(connection)
	gplog.FatalOnError(err)
	return segConfigs
}
