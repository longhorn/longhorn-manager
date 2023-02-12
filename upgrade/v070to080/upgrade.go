package v070to080

import (
	"fmt"

	"github.com/pkg/errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
	upgradeutil "github.com/longhorn/longhorn-manager/upgrade/util"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	upgradeLogPrefix = "upgrade from v0.7.0 to v0.8.0: "
)

func migrateEngineBinaries() (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"migrate engine binaries failed")
	}()
	return util.CopyHostDirectoryContent(DeprecatedEngineBinaryDirectoryOnHost, types.EngineBinaryDirectoryOnHost)
}

func UpgradeLocalNode() error {
	if err := migrateEngineBinaries(); err != nil {
		return err
	}
	return nil
}

func UpgradeResources(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) error {
	if err := doInstanceManagerUpgrade(namespace, lhClient, resourceMaps); err != nil {
		return err
	}
	return nil
}

func doInstanceManagerUpgrade(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade instance manager failed")
	}()

	nodeMap, err := upgradeutil.ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all nodes during the instance managers upgrade")
	}

	imMap, err := upgradeutil.ListAndUpdateInstanceManagersInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, upgradeLogPrefix+"failed to list all existing instance managers during the instance managers upgrade")
	}

	for _, im := range imMap {
		if im.Spec.Image != "" {
			continue
		}
		if types.ValidateEngineImageChecksumName(im.Spec.EngineImage) {
			ei, err := upgradeutil.GetEngineImageFromProvidedCache(namespace, lhClient, resourceMaps, im.Spec.EngineImage)
			if err != nil {
				return errors.Wrapf(err, upgradeLogPrefix+"failed to find out the related engine image %v during the instance managers upgrade", im.Spec.EngineImage)
			}
			im.Spec.EngineImage = ei.Spec.Image
		}
		im.Spec.Image = im.Spec.EngineImage
		node, exist := nodeMap[im.Spec.NodeID]
		if !exist {
			return fmt.Errorf(upgradeLogPrefix+"cannot to find node %v for instance manager %v during the instance manager upgrade", im.Spec.NodeID, im.Name)
		}
		metadata, err := meta.Accessor(im)
		if err != nil {
			return err
		}
		metadata.SetOwnerReferences(datastore.GetOwnerReferencesForNode(node))
		metadata.SetLabels(types.GetInstanceManagerLabels(im.Spec.NodeID, im.Spec.Image, im.Spec.Type))

		im.Status.APIMinVersion = engineapi.IncompatibleInstanceManagerAPIVersion
		im.Status.APIVersion = engineapi.IncompatibleInstanceManagerAPIVersion
	}

	return nil
}
