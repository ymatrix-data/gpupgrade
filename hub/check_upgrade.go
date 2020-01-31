package hub

import (
	"sync"

	"github.com/pkg/errors"

	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/step"
)

func (s *Server) CheckUpgrade(stream step.OutStreams) error {
	var wg sync.WaitGroup
	checkErrs := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()

		stateDir := s.StateDir
		err := UpgradeMaster(s.Source, s.Target, stateDir, stream, true, false)
		if err != nil {
			checkErrs <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		agentConns, agentConnsErr := s.AgentConns()

		if agentConnsErr != nil {
			checkErrs <- errors.Wrap(agentConnsErr, "failed to connect to gpupgrade agent")
		}

		dataDirPairMap, dataDirPairsErr := s.GetDataDirPairs()

		if dataDirPairsErr != nil {
			checkErrs <- errors.Wrap(dataDirPairsErr, "failed to get old and new primary data directories")
		}

		upgradeErr := UpgradePrimaries(true, "", agentConns, dataDirPairMap, s.Source, s.Target, s.UseLinkMode)

		if upgradeErr != nil {
			checkErrs <- upgradeErr
		}
	}()

	wg.Wait()
	close(checkErrs)

	var multiErr *multierror.Error
	for err := range checkErrs {
		multiErr = multierror.Append(multiErr, err)
	}

	return multiErr.ErrorOrNil()
}
