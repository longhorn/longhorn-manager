package spdk

import (
	"os"
	"strings"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// getUblkTargetCPUMask returns the CPU mask for the SPDK ublk target pollers. The instance manager keeps
// the SPDK admin reactor on the first core of the SPDK CPU mask and exports the other cores in
// types.EnvSPDKPollerCPUMask, so the ublk target stays off the admin reactor core. An empty value makes
// SPDK use all cores, which is the behavior when the variable is not set.
func getUblkTargetCPUMask() string {
	return strings.TrimSpace(os.Getenv(types.EnvSPDKPollerCPUMask))
}

// createUblkTarget creates the SPDK ublk target pinned to the poller cores. The ublk target is a
// singleton and the first creation decides its CPU mask, so this must run before anything else
// (for example the initiator in go-spdk-helper, which creates it with an empty mask) creates it.
// Creating an already existing target is treated as success by the client.
func createUblkTarget(spdkClient *spdkclient.Client) error {
	return spdkClient.UblkCreateTarget(getUblkTargetCPUMask(), true)
}
