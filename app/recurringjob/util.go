package recurringjob

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/client-go/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

func filterSnapshotCRs(snapshotCRs []longhornclient.SnapshotCR, predicate func(snapshot longhornclient.SnapshotCR) bool) []longhornclient.SnapshotCR {
	filtered := []longhornclient.SnapshotCR{}
	for _, snapshotCR := range snapshotCRs {
		if predicate(snapshotCR) {
			filtered = append(filtered, snapshotCR)
		}
	}
	return filtered
}

// filterSnapshotCRsWithLabel return snapshotCRs that have LabelKey and LabelValue
func filterSnapshotCRsWithLabel(snapshotCRs []longhornclient.SnapshotCR, labelKey, labelValue string) []longhornclient.SnapshotCR {
	return filterSnapshotCRs(snapshotCRs, func(snapshotCR longhornclient.SnapshotCR) bool {
		snapshotLabelValue, found := snapshotCR.Labels[labelKey]
		return found && labelValue == snapshotLabelValue
	})
}

// filterSnapshotCRsNotInTargets returns snapshots that are not in the Targets
func filterSnapshotCRsNotInTargets(snapshotCRs []longhornclient.SnapshotCR, targets map[string]struct{}) []longhornclient.SnapshotCR {
	return filterSnapshotCRs(snapshotCRs, func(snapshotCR longhornclient.SnapshotCR) bool {
		if _, ok := targets[snapshotCR.Name]; !ok {
			return true
		}
		return false
	})
}

// filterExpiredItems returns a list of names from the input sts excluding the latest retainCount names
func filterExpiredItems(nts []NameWithTimestamp, retainCount int) []string {
	sort.Slice(nts, func(i, j int) bool {
		return nts[i].Timestamp.Before(nts[j].Timestamp)
	})

	ret := []string{}
	for i := 0; i < len(nts)-retainCount; i++ {
		ret = append(ret, nts[i].Name)
	}
	return ret
}

func snapshotCRsToNameWithTimestamps(snapshotCRs []longhornclient.SnapshotCR) []NameWithTimestamp {
	result := []NameWithTimestamp{}
	for _, snapshotCR := range snapshotCRs {
		t, err := time.Parse(time.RFC3339, snapshotCR.CrCreationTime)
		if err != nil {
			logrus.Errorf("Failed to parse datetime %v for snapshot CR %v",
				snapshotCR.CrCreationTime, snapshotCR.Name)
			continue
		}
		result = append(result, NameWithTimestamp{
			Name:      snapshotCR.Name,
			Timestamp: t,
		})
	}
	return result
}

func snapshotCRsToNames(snapshotCRs []longhornclient.SnapshotCR) []string {
	result := []string{}
	for _, snapshotCR := range snapshotCRs {
		result = append(result, snapshotCR.Name)
	}
	return result
}

func filterVolumesForJob(allowDetached bool, volumes []longhorn.Volume, filterNames *[]string) {
	logger := logrus.StandardLogger()
	for _, volume := range volumes {
		// skip duplicates
		if util.Contains(*filterNames, volume.Name) {
			continue
		}

		if volume.Status.RestoreRequired {
			logger.Infof("Bypassed to create job for %v volume during restoring from the backup", volume.Name)
			continue
		}

		if volume.Status.Robustness != longhorn.VolumeRobustnessFaulted &&
			(volume.Status.State == longhorn.VolumeStateAttached || allowDetached) {
			*filterNames = append(*filterNames, volume.Name)
			continue
		}
		logger.Warnf("Cannot create job for %v volume in state %v", volume.Name, volume.Status.State)
	}
}

func getVolumesBySelector(recurringJobType, recurringJobName, namespace string, client *lhclientset.Clientset) ([]longhorn.Volume, error) {
	logger := logrus.StandardLogger()

	label := fmt.Sprintf("%s=%s",
		types.GetRecurringJobLabelKey(recurringJobType, recurringJobName), types.LonghornLabelValueEnabled)
	logger.Infof("Got volumes from label %v", label)

	volumes, err := client.LonghornV1beta2().Volumes(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: label,
	})
	if err != nil {
		return nil, err
	}
	return volumes.Items, nil
}

func getSettingAsBoolean(name types.SettingName, namespace string, client *lhclientset.Clientset) (bool, error) {
	obj, err := client.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(name), metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	value, err := strconv.ParseBool(obj.Value)
	if err != nil {
		return false, err
	}
	return value, nil
}

func GetLonghornClientset() (*lhclientset.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get client config")
	}
	return lhclientset.NewForConfig(config)
}

func sliceStringSafely(s string, begin, end int) string {
	if begin < 0 {
		begin = 0
	}
	if end > len(s) {
		end = len(s)
	}
	return s[begin:end]
}

func systemBackupsToNameWithTimestamps(systemBackupList *longhorn.SystemBackupList) []NameWithTimestamp {
	result := []NameWithTimestamp{}
	for _, systemBackup := range systemBackupList.Items {
		result = append(result, NameWithTimestamp{
			Name:      systemBackup.Name,
			Timestamp: systemBackup.Status.CreatedAt.Time,
		})
	}
	return result
}
