package v110to120

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	batchv1beta1 "k8s.io/api/batch/v1beta1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

const (
	upgradeLogPrefix = "upgrade from v1.1.0 to v1.2.0: "
)

func UpgradeCRs(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) error {
	if err := upgradeRecurringJobs(namespace, lhClient, kubeClient); err != nil {
		return err
	}
	return nil
}

func upgradeRecurringJobs(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"upgrade volume failed")
	}()

	// Need to transfer volume spec recurringJobs to CRs because Longhorn
	// is migrating to label-driven recurring job CRD.
	idMapSpec := make(map[string]*types.RecurringJobSpec)
	err = translateStorageClassRecurringJobsToSelector(namespace, kubeClient, idMapSpec)
	if err != nil {
		return err
	}
	err = translateVolumeRecurringJobsToLabel(namespace, lhClient, idMapSpec)
	if err != nil {
		return err
	}
	err = translateVolumeRecurringJobToCRs(namespace, lhClient, idMapSpec)
	if err != nil {
		return err
	}

	err = cleanupAppliedVolumeCronJobs(namespace, lhClient, kubeClient)
	if err != nil {
		return err
	}
	return nil
}

type recurringJobSelector struct {
	Name    string `json:"name"`
	IsGroup bool   `json:"isGroup"`
}

func translateStorageClassRecurringJobsToSelector(namespace string, kubeClient *clientset.Clientset, sharedMap map[string]*types.RecurringJobSpec) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"translate storageClass recurringJobs to recurringJobSelector failed")
	}()

	log := logrus.WithFields(
		logrus.Fields{
			"upgrade":   "translate-storageClass-recurringJobs-to-recurringJobSelector",
			"namespace": namespace,
		},
	)

	log.Debugf("Getting %v configMap", types.DefaultStorageClassConfigMapName)
	storageClassCM, err := kubeClient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), types.DefaultStorageClassConfigMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get %v ConfigMap: %v", types.DefaultStorageClassConfigMapName, err)
	}
	storageClassYAML, found := storageClassCM.Data["storageclass.yaml"]
	if !found {
		return fmt.Errorf("failed to find storageclass.yaml inside the %v ConfigMap", types.DefaultStorageClassConfigMapName)
	}
	sc, err := buildStorageClassFromYAML(storageClassYAML)
	if err != nil {
		return err
	}
	scRecurringJobsJSON, ok := sc.Parameters["recurringJobs"]
	if !ok {
		return nil
	}
	scRecurringJobs := []types.RecurringJobSpec{}
	err = json.Unmarshal([]byte(scRecurringJobsJSON), &scRecurringJobs)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal: %v", scRecurringJobs)
	}

	recurringJobIDs := []string{}
	for _, recurringJob := range scRecurringJobs {
		id, err := createRecurringJobID(recurringJob)
		if err != nil {
			return errors.Wrapf(err, "failed to create ID for recurring job %v", recurringJob)
		}
		sharedMap[id] = &types.RecurringJobSpec{
			Name:        id,
			Task:        recurringJob.Task,
			Cron:        recurringJob.Cron,
			Retain:      recurringJob.Retain,
			Concurrency: types.DefaultRecurringJobConcurrency,
			Labels:      recurringJob.Labels,
		}
		if !util.Contains(recurringJobIDs, id) {
			recurringJobIDs = append(recurringJobIDs, id)
		}
	}
	log.Debugf("Converting recurringJobs %v to recurringJobSelector", recurringJobIDs)
	scRecurringJobSelectors := []recurringJobSelector{}
	scRecurringJobSelectorJSON, ok := sc.Parameters["recurringJobSelector"]
	scRecurringJobSelectorIDs := []string{}
	if ok {
		err = json.Unmarshal([]byte(scRecurringJobSelectorJSON), &scRecurringJobSelectors)
		if err != nil {
			return errors.Wrapf(err, "failed to unmarshal: %v", scRecurringJobs)
		}
		for _, selector := range scRecurringJobSelectors {
			scRecurringJobSelectorIDs = append(scRecurringJobSelectorIDs, selector.Name)
		}
	}
	for _, id := range recurringJobIDs {
		if util.Contains(scRecurringJobSelectorIDs, id) {
			continue
		}
		scRecurringJobSelectors = append(scRecurringJobSelectors, recurringJobSelector{
			Name:    id,
			IsGroup: false,
		})
	}
	selectorJSON, err := json.Marshal(scRecurringJobSelectors)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal JSON %v", scRecurringJobSelectors)
	}
	log.Infof("Adding %v to recurringJobSelector", recurringJobIDs)
	sc.Parameters["recurringJobSelector"] = string(selectorJSON)
	log.Debug("Removing recurringJobs")
	delete(sc.Parameters, "recurringJobs")

	newStorageClassYAML, err := yaml.Marshal(sc)
	storageClassCM.Data["storageclass.yaml"] = string(newStorageClassYAML)
	logrus.Infof("Updating %v configmap", storageClassCM.Name)
	if _, err := kubeClient.CoreV1().ConfigMaps(namespace).Update(context.TODO(), storageClassCM, metav1.UpdateOptions{}); err != nil {
		return errors.Wrapf(err, "failed to update %v configmap", storageClassCM.Name)
	}
	return nil
}

func translateVolumeRecurringJobsToLabel(namespace string, lhClient *lhclientset.Clientset, sharedMap map[string]*types.RecurringJobSpec) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"translate volume recurringJobs to volume labels failed")
	}()

	log := logrus.WithFields(
		logrus.Fields{
			"upgrade":   "translate-volume-recurringJobs-to-volume-labels",
			"namespace": namespace,
		},
	)

	log.Debugf("Listing all volumes")
	volumeList, err := lhClient.LonghornV1beta1().Volumes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Cannot find volumes")
			return nil
		}
		return errors.Wrap(err, "failed to list volumes")
	}
	for _, volume := range volumeList.Items {
		volumeLabels := volume.Labels
		if volumeLabels == nil {
			volumeLabels = map[string]string{}
		}

		for _, recurringJob := range volume.Spec.RecurringJobs {
			recurringJobSpec := types.RecurringJobSpec{
				Name:        recurringJob.Name,
				Task:        recurringJob.Task,
				Cron:        recurringJob.Cron,
				Retain:      recurringJob.Retain,
				Concurrency: types.DefaultRecurringJobConcurrency,
				Labels:      recurringJob.Labels,
			}
			id, err := createRecurringJobID(recurringJobSpec)
			if err != nil {
				return errors.Wrapf(err, "failed to create ID for recurring job %v", recurringJob)
			}
			if _, exist := sharedMap[id]; !exist {
				recurringJobSpec.Name = id
				sharedMap[id] = &recurringJobSpec
			}
			key := types.GetRecurringJobLabelKey(types.LonghornLabelRecurringJob, id)
			volumeLabels[key] = types.LonghornLabelValueEnabled
		}
		if len(volumeLabels) != 0 {
			volume.Labels = volumeLabels
			volume.Spec.RecurringJobs = nil
		}
		logrus.Infof("Updating %v volume labels to %v", volume.Name, volume.Labels)
		updatedVolume, err := lhClient.LonghornV1beta1().Volumes(namespace).Update(context.TODO(), &volume, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to update %v volume", volume.Name)
		}
		volume = *updatedVolume
	}
	return nil
}

func translateVolumeRecurringJobToCRs(namespace string, lhClient *lhclientset.Clientset, sharedMap map[string]*types.RecurringJobSpec) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"translate volume recurringJobs to recurringJob CRs")
	}()

	log := logrus.WithFields(
		logrus.Fields{
			"upgrade":   "translate-volume-recurringJobs-to-recurringJob-CRs",
			"namespace": namespace,
		},
	)

	if len(sharedMap) == 0 {
		log.Debug("Found 0 recurring job needs to be converted to CR")
		return nil
	}

	for _, spec := range sharedMap {
		log.Infof("Creating %v recurring job CR", spec.Name)
		job := &longhorn.RecurringJob{
			ObjectMeta: metav1.ObjectMeta{
				Name: spec.Name,
			},
			Spec: *spec,
		}
		log.Debugf("Checking if %v recurring job CR already exists", spec.Name)
		obj, err := lhClient.LonghornV1beta1().RecurringJobs(namespace).Get(context.TODO(), job.Name, metav1.GetOptions{})
		if err == nil {
			log.Debugf("Recurring job CR already exists %v", obj)
			continue
		}
		if !apierrors.IsNotFound(err) {
			log.Debugf("Failed to get recurring job %v", spec.Name)
		}
		_, err = lhClient.LonghornV1beta1().RecurringJobs(namespace).Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			return errors.Wrapf(err, "failed to create recurring job CR with %v", spec)
		}
	}
	return nil
}

func cleanupAppliedVolumeCronJobs(namespace string, lhClient *lhclientset.Clientset, kubeClient *clientset.Clientset) (err error) {
	defer func() {
		err = errors.Wrapf(err, upgradeLogPrefix+"cleanup applied volume cron jobs")
	}()

	log := logrus.WithFields(
		logrus.Fields{
			"upgrade":   "cleanup-applied-volume-cron-jobs",
			"namespace": namespace,
		},
	)

	log.Debugf("Listing all volumes")
	volumeList, err := lhClient.LonghornV1beta1().Volumes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Cannot find volumes")
			return nil
		}
		return errors.Wrap(err, "failed to list volumes")
	}

	propagation := metav1.DeletePropagationForeground
	cronJobClient := kubeClient.BatchV1beta1().CronJobs(namespace)
	for _, v := range volumeList.Items {
		log.Debugf("Listing all cron jobs for volume %v", v.Name)
		appliedCronJobROs, err := listVolumeCronJobROs(v.Name, namespace, kubeClient)
		if err != nil {
			return errors.Wrapf(err, "failed to list all cron jobs for volume %v", v.Name)
		}
		for name := range appliedCronJobROs {
			log.Infof("Deleting %v cronjob job for %v volume", name, v.Name)
			err := cronJobClient.Delete(context.TODO(), name, metav1.DeleteOptions{PropagationPolicy: &propagation})
			if err != nil {
				return errors.Wrapf(err, "failed to delete %v cron job for volume %v", name, v.Name)
			}
		}
		log.Infof("Deleted %v cronjob job for %v volume", len(appliedCronJobROs), v.Name)
	}
	return nil
}

func buildStorageClassFromYAML(storageclassYAML string) (*storagev1.StorageClass, error) {
	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, _, err := decode([]byte(storageclassYAML), nil, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decoding YAML string")
	}
	storageclass, ok := obj.(*storagev1.StorageClass)
	if !ok {
		return nil, fmt.Errorf("invalid storageclass YAML string: %v", storageclassYAML)
	}
	return storageclass, nil
}

func createRecurringJobID(recurringJob types.RecurringJobSpec) (key string, err error) {
	labelJSON, err := json.Marshal(recurringJob.Labels)
	if err != nil {
		return key, errors.Wrapf(err, "failed to marshal JSON %v", recurringJob.Labels)
	}
	return fmt.Sprintf("%v-%v-%v-%v",
		recurringJob.Task,
		recurringJob.Retain,
		util.GetStringHash(recurringJob.Cron),
		util.GetStringHash(string(labelJSON)),
	), nil
}

func listVolumeCronJobROs(volumeName, namespace string, kubeClient *clientset.Clientset) (map[string]*batchv1beta1.CronJob, error) {
	itemMap := map[string]*batchv1beta1.CronJob{}
	list, err := kubeClient.BatchV1beta1().CronJobs(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: types.LonghornLabelVolume + "=" + volumeName,
	})
	if err != nil {
		return nil, err
	}
	for _, cj := range list.Items {
		itemMap[cj.Name] = &cj
	}
	return itemMap, nil
}
