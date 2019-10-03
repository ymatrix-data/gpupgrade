package commanders

import (
	"context"

	"github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func Finalize(client idl.CliToHubClient) error {
	_, err := client.Finalize(context.Background(), &idl.FinalizeRequest{})
	if err != nil {
		gplog.Error(err.Error())
		return err
	}

	gplog.Info("Request to reconfigure master port on upgraded cluster complete")
	return nil
}
