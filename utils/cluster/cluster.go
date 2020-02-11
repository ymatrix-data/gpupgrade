package cluster

/*
 * This file contains structs and functions related to interacting
 * with files and directories, both locally and remotely over SSH.
 *
 * It was originally copied from gp-common-go-libs and will, hopefully, be
 * slowly merged into and subsumed by the upgrade Cluster object.
 */

import (
	"errors"
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type Cluster struct {
	// ContentIDs contains the list of all primary content IDs, in the same
	// order that they were provided to NewCluster. Clients requiring a stable
	// iteration order over the Primaries map may use this.
	ContentIDs []int

	// Primaries contains the primary SegConfigs, keyed by content ID. One
	// primary exists for every entry in ContentIDs.
	Primaries map[int]SegConfig

	// Mirrors contains any mirror SegConfigs, keyed by content ID. Not every
	// primary is guaranteed to have a corresponding mirror, so lookups should
	// check for key existence.
	Mirrors map[int]SegConfig
}

type SegConfig struct {
	DbID      int
	ContentID int
	Port      int
	Hostname  string
	DataDir   string
	Role      string
}

const (
	PrimaryRole = "p"
	MirrorRole  = "m"
)

// ErrInvalidSegments is returned by NewCluster if the segment configuration
// array does not map to a valid cluster.
var ErrInvalidSegments = errors.New("invalid segment configuration")

/*
 * Base cluster functions
 */

func NewCluster(segConfigs []SegConfig) (*Cluster, error) {
	cluster := Cluster{}

	cluster.Primaries = make(map[int]SegConfig)
	cluster.Mirrors = make(map[int]SegConfig)

	for _, seg := range segConfigs {
		content := seg.ContentID

		switch seg.Role {
		case PrimaryRole:
			// Check for duplication.
			if _, ok := cluster.Primaries[content]; ok {
				return nil, newInvalidSegmentsError(seg, "multiple primaries with content ID %d", content)
			}

			cluster.ContentIDs = append(cluster.ContentIDs, content)
			cluster.Primaries[content] = seg

		case MirrorRole:
			// Check for duplication.
			if _, ok := cluster.Mirrors[content]; ok {
				return nil, newInvalidSegmentsError(seg, "multiple mirrors with content ID %d", content)
			}

			cluster.Mirrors[content] = seg

		default:
			return nil, newInvalidSegmentsError(seg, "unknown role %q", seg.Role)
		}
	}

	// Make sure each mirror has a primary.
	for _, seg := range cluster.Mirrors {
		content := seg.ContentID

		if _, ok := cluster.Primaries[content]; !ok {
			return nil, newInvalidSegmentsError(seg, "mirror with content ID %d has no primary", content)
		}
	}

	return &cluster, nil
}

func (cluster *Cluster) GetContentList() []int {
	return cluster.ContentIDs
}

func (cluster *Cluster) GetDbidForContent(contentID int) int {
	return cluster.Primaries[contentID].DbID
}

func (cluster *Cluster) GetPortForContent(contentID int) int {
	return cluster.Primaries[contentID].Port
}

func (cluster *Cluster) GetHostForContent(contentID int) string {
	return cluster.Primaries[contentID].Hostname
}

func (cluster *Cluster) GetDirForContent(contentID int) string {
	return cluster.Primaries[contentID].DataDir
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
	e.fselocation as datadir,
	s.role
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE f.fsname = 'pg_system'
ORDER BY s.content;`
	} else {
		query = `
SELECT
	dbid,
	content as contentid,
	port,
	hostname,
	datadir,
	role
FROM gp_segment_configuration
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

// InvalidSegmentsError is the backing error type for ErrInvalidSegments. It
// contains the offending configuration object.
type InvalidSegmentsError struct {
	Segment SegConfig

	msg string
}

func newInvalidSegmentsError(seg SegConfig, format string, a ...interface{}) *InvalidSegmentsError {
	return &InvalidSegmentsError{
		Segment: seg,
		msg:     fmt.Sprintf(format, a...),
	}
}

func (i *InvalidSegmentsError) Error() string {
	return fmt.Sprintf("invalid segment configuration (%+v): %s", i.Segment, i.msg)
}

func (i *InvalidSegmentsError) Is(err error) bool {
	return err == ErrInvalidSegments
}
