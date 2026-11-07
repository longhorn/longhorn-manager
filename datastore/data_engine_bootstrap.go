package datastore

import (
	"context"

	"github.com/cockroachdb/errors"

	"k8s.io/client-go/kubernetes"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

// InitializeDataEngineModeSettings creates missing compatible engine Settings
// only after an uncached bootstrap preflight. It never rewrites an existing one.
func InitializeDataEngineModeSettings(ctx context.Context, mode types.DataEngineMode, namespace string, lhClient lhclientset.Interface, kubeClient kubernetes.Interface) error {
	parsed, err := types.ParseDataEngineMode(string(mode))
	if err != nil {
		return err
	}
	if parsed != types.DataEngineModeV2Only {
		return nil
	}
	if ctx == nil || namespace == "" || lhClient == nil || kubeClient == nil {
		return errors.New("v2-only initialization requires a context, namespace and both API clients")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	known, err := readDataEngineModeSettings(ctx, namespace, lhClient, true)
	if err != nil {
		return err
	}
	readUnchanged := func(previous [2]dataEngineSettingIdentity) ([2]dataEngineSettingIdentity, error) {
		current, err := readDataEngineModeSettings(ctx, namespace, lhClient, true)
		if err != nil {
			return current, err
		}
		for index, identity := range previous {
			if identity.uid != "" && identity != current[index] {
				return current, errors.New("engine Setting identity changed during v2-only initialization")
			}
		}
		return current, nil
	}
	for index, name := range []types.SettingName{types.SettingNameV1DataEngine, types.SettingNameV2DataEngine} {
		if err := ValidateDataEngineModeBootstrapState(ctx, parsed, namespace, lhClient, kubeClient); err != nil {
			return err
		}
		known, err = readUnchanged(known)
		if err != nil {
			return err
		}
		if known[index].uid != "" {
			continue
		}
		created, err := lhClient.LonghornV1beta2().Settings(namespace).Create(ctx, &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: namespace},
			Value:      v2OnlyEngineSettingValue(name),
		}, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create compatible %s Setting", name)
		}
		if err == nil {
			known[index], err = dataEngineModeSettingIdentity(created, name, namespace)
			if err != nil {
				return err
			}
		}
		known, err = readUnchanged(known)
		if err != nil {
			return err
		}
		if known[index].uid == "" {
			return errors.New("engine Setting creation did not leave a compatible persisted member")
		}
	}
	return ValidateDataEngineModeState(ctx, parsed, namespace, lhClient, kubeClient)
}
