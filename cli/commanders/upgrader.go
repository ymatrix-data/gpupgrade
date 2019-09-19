package commanders

import (
	"context"

	"github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type Upgrader struct {
	client idl.CliToHubClient
}

func NewUpgrader(client idl.CliToHubClient) *Upgrader {
	return &Upgrader{client: client}
}

func (u *Upgrader) ReconfigurePorts() error {
	_, err := u.client.UpgradeReconfigurePorts(context.Background(), &idl.UpgradeReconfigurePortsRequest{})
	if err != nil {
		gplog.Error(err.Error())
		return err
	}

	gplog.Info("Request to reconfigure master port on upgraded cluster complete")
	return nil
}
