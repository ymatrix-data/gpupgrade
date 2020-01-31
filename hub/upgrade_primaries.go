package hub

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

func UpgradePrimaries(checkOnly bool, masterBackupDir string, agentConns []*Connection, dataDirPairMap map[string][]*idl.DataDirPair, source *utils.Cluster, target *utils.Cluster, useLinkMode bool) error {
	wg := sync.WaitGroup{}
	agentErrs := make(chan error, len(agentConns))
	for _, agentConn := range agentConns {
		wg.Add(1)

		go func(conn *Connection) {
			defer wg.Done()

			_, err := idl.NewAgentClient(conn.Conn).UpgradePrimaries(context.Background(), &idl.UpgradePrimariesRequest{
				SourceBinDir:    source.BinDir,
				TargetBinDir:    target.BinDir,
				TargetVersion:   target.Version.SemVer.String(),
				DataDirPairs:    dataDirPairMap[conn.Hostname],
				CheckOnly:       checkOnly,
				UseLinkMode:     useLinkMode,
				MasterBackupDir: masterBackupDir,
			})

			if err != nil {
				agentErrs <- errors.Wrapf(err, "gpupgrade agent failed to convert primary segment on host %s", conn.Hostname)
			}
		}(agentConn)
	}

	wg.Wait()
	close(agentErrs)

	var err error

	for agentErr := range agentErrs {
		err = multierror.Append(err, agentErr)
	}

	return err
}

func (s *Server) GetDataDirPairs() (map[string][]*idl.DataDirPair, error) {
	dataDirPairMap := make(map[string][]*idl.DataDirPair)

	sourceContents := s.Source.ContentIDs
	targetContents := s.Target.ContentIDs
	if len(sourceContents) != len(targetContents) {
		return nil, fmt.Errorf("old and new cluster content identifiers do not match")
	}
	sort.Ints(sourceContents)
	sort.Ints(targetContents)
	for i := range sourceContents {
		if sourceContents[i] != targetContents[i] {
			return nil, fmt.Errorf("old and new cluster content identifiers do not match")
		}
	}

	for _, contentID := range s.Source.ContentIDs {
		if contentID == -1 {
			continue
		}
		sourceSeg := s.Source.Segments[contentID]
		targetSeg := s.Target.Segments[contentID]
		if sourceSeg.Hostname != targetSeg.Hostname {
			return nil, fmt.Errorf("hostnames do not match between old and new cluster with content ID %d. Found old cluster hostname: '%s', and new cluster hostname: '%s'", contentID, sourceSeg.Hostname, targetSeg.Hostname)
		}

		dataPair := &idl.DataDirPair{
			SourceDataDir: sourceSeg.DataDir,
			TargetDataDir: targetSeg.DataDir,
			SourcePort:    int32(sourceSeg.Port),
			TargetPort:    int32(targetSeg.Port),
			Content:       int32(contentID),
			DBID:          int32(sourceSeg.DbID),
		}

		dataDirPairMap[sourceSeg.Hostname] = append(dataDirPairMap[sourceSeg.Hostname], dataPair)
	}

	return dataDirPairMap, nil
}
