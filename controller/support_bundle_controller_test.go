package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	TestSupportBundleName              = "support-bundle"
	TestSupportBundleNameFailSuffix    = "-fail"
	TestSupportBundleNamePurgeSuffix   = "-purge"
	TestSupportBundleNameDeleteSuffix  = "-delete"
	TestSupportBundleNameReadySuffix   = "-ready"
	TestSupportBundleNameReplaceSuffix = "-replace"

	TestSupportBundleManagerPodIP   = "10.10.10.10"
	TestSupportBundleManagerPodName = "support-bundle-manager"

	TestSupportBundleServiceAccount = "longhorn-support-bundle"
)

type SupportBundleTestCase struct {
	state longhorn.SupportBundleState

	controllerID string

	supportBundleNames              []string
	supportBundleManagerPodNames    []string
	supportBundleFailedHistoryLimit string

	expectedState    longhorn.SupportBundleState
	expectedNotExist bool
}

func (s *TestSuite) TestReconcileSupportBundle(c *C) {
	rolloutOwnerID := TestNode1
	tempDirs := []string{}
	defer func() {
		for _, dir := range tempDirs {
			err := os.RemoveAll(dir)
			c.Assert(err, IsNil)
		}
	}()

	testCases := map[string]SupportBundleTestCase{
		"support bundle create": {
			state:         longhorn.SupportBundleStateNone,
			expectedState: longhorn.SupportBundleStateStarted,
		},
		"support bundle create while ready support bundle exists": {
			state: longhorn.SupportBundleStateNone,
			supportBundleNames: []string{
				TestSupportBundleName,
				TestSupportBundleName + TestSupportBundleNameReadySuffix,
			},
			expectedState: longhorn.SupportBundleStateStarted,
		},
		"support bundle start deploy support bundle manager": {
			state:         longhorn.SupportBundleStateStarted,
			expectedState: longhorn.SupportBundleStateStarted,
		},
		"support bundle start deployed support bundle manager": {
			state:                        longhorn.SupportBundleStateStarted,
			expectedState:                longhorn.SupportBundleStateGenerating,
			supportBundleManagerPodNames: []string{TestSupportBundleManagerPodName},
		},
		"support bundle generating": {
			state:                        longhorn.SupportBundleStateGenerating,
			expectedState:                longhorn.SupportBundleStateReady,
			supportBundleManagerPodNames: []string{TestSupportBundleManagerPodName},
		},
		"support bundle deleting": {
			state: longhorn.SupportBundleStateReady,
			supportBundleNames: []string{
				TestSupportBundleName + TestSupportBundleNameDeleteSuffix,
			},
			expectedState: longhorn.SupportBundleStateDeleting,
		},
		"support bundle deleted": {
			state:            longhorn.SupportBundleStateDeleting,
			expectedNotExist: true,
		},
		"support bundle failed with support-bundle-failed-history-limit set to 0": {
			state: longhorn.SupportBundleStateGenerating,
			supportBundleNames: []string{
				TestSupportBundleName + TestSupportBundleNamePurgeSuffix,
			},
			supportBundleFailedHistoryLimit: "0",
			expectedState:                   longhorn.SupportBundleStatePurging,
		},
		"support bundle purge": {
			state: longhorn.SupportBundleStatePurging,
			supportBundleNames: []string{
				TestSupportBundleName + TestSupportBundleNamePurgeSuffix,
			},
			expectedNotExist: true,
		},
		"support bundle replaced": {
			state: longhorn.SupportBundleStateReplaced,
			supportBundleNames: []string{
				TestSupportBundleName + TestSupportBundleNameReplaceSuffix,
			},
			expectedNotExist: true,
		},
		"support bundle change ownership": {
			state:         longhorn.SupportBundleStateNone,
			controllerID:  TestNode2,
			expectedState: longhorn.SupportBundleStateStarted,
		},
	}

	for name, tc := range testCases {
		if len(tc.supportBundleNames) == 0 {
			tc.supportBundleNames = append(tc.supportBundleNames, TestSupportBundleName)
		}

		if tc.expectedState == "" {
			tc.expectedState = tc.state
		}

		if tc.supportBundleFailedHistoryLimit == "" {
			tc.supportBundleFailedHistoryLimit = "1"
		}

		if tc.controllerID == "" {
			tc.controllerID = rolloutOwnerID
		}

		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset()
		extensionsClient := apiextensionsfake.NewSimpleClientset()

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		fakeSupportBundleNamespace(c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSupportBundleManagerImageSetting(c, informerFactories.LhInformerFactory, lhClient)
		fakeSupportBundleFailedHistoryLimitSetting(tc.supportBundleFailedHistoryLimit, c, informerFactories.LhInformerFactory, lhClient)

		supportBundleController, err := newFakeSupportBundleController(lhClient, kubeClient, extensionsClient, informerFactories, tc.controllerID)
		c.Assert(err, IsNil)

		var supportBundle *longhorn.SupportBundle
		for _, supportBundleName := range tc.supportBundleNames {
			supportBundle = fakeSupportBundle(supportBundleName, rolloutOwnerID, tc.state, c, informerFactories.LhInformerFactory, lhClient)

			if strings.HasSuffix(supportBundleName, TestSupportBundleNamePurgeSuffix) {
				continue
			}

			for _, podName := range tc.supportBundleManagerPodNames {
				fakeSupportBundleManagerPod(podName, supportBundle, supportBundleController.ds, c, informerFactories.KubeInformerFactory, kubeClient)
			}
		}
		c.Assert(supportBundle, NotNil)

		err = supportBundleController.reconcile(tc.supportBundleNames[0])
		c.Assert(err, IsNil)

		for _, supportBundleName := range tc.supportBundleNames {
			supportBundle, err = lhClient.LonghornV1beta2().SupportBundles(TestNamespace).Get(context.TODO(), supportBundleName, metav1.GetOptions{})
			isPurged := tc.state == longhorn.SupportBundleStatePurging && strings.HasSuffix(supportBundleName, TestSupportBundleNamePurgeSuffix)
			isReplaced := tc.state == longhorn.SupportBundleStateReplaced && strings.HasSuffix(supportBundleName, TestSupportBundleNameReplaceSuffix)
			if isPurged || isReplaced {
				c.Assert(err, NotNil)
				c.Assert(apierrors.IsNotFound(err), Equals, tc.expectedNotExist)
				continue
			}

			if tc.state == longhorn.SupportBundleStateNone && strings.HasSuffix(supportBundleName, TestSupportBundleNameReadySuffix) {
				c.Assert(err, IsNil)
				c.Assert(supportBundle.Status.State, Equals, longhorn.SupportBundleStateReplaced)
				continue
			}

			c.Assert(err, IsNil)
			c.Assert(supportBundle.Status.State, Equals, tc.expectedState)

			isFinalizer := util.FinalizerExists(longhornFinalizerKey, supportBundle) == !tc.expectedNotExist
			c.Assert(isFinalizer, Equals, true)
		}
	}
}

func newFakeSupportBundleController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*SupportBundleController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	c, err := NewSupportBundleController(logger, ds, scheme.Scheme, kubeClient, controllerID, TestNamespace, TestSupportBundleServiceAccount)
	if err != nil {
		return nil, err
	}
	c.eventRecorder = record.NewFakeRecorder(100)
	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	c.httpClient = &fakeSupportBundleHTTPClient{}

	return c, nil
}

func fakeSupportBundle(name, currentOwnerID string, state longhorn.SupportBundleState, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) *longhorn.SupportBundle {
	if strings.HasSuffix(name, TestSupportBundleNameFailSuffix) {
		state = longhorn.SupportBundleStateError
	}

	if strings.HasSuffix(name, TestSupportBundleNameReadySuffix) {
		state = longhorn.SupportBundleStateReady
	}

	var deletionTimestamp metav1.Time
	if strings.HasSuffix(name, TestSupportBundleNameDeleteSuffix) {
		deletionTimestamp = metav1.NewTime(time.Now())
	}

	supportBundle := newSupportBundle(name, "", "", currentOwnerID, state, &deletionTimestamp)
	err := util.AddFinalizer(longhornFinalizerKey, supportBundle)
	c.Assert(err, IsNil)

	supportBundle, err = client.LonghornV1beta2().SupportBundles(TestNamespace).Create(context.TODO(), supportBundle, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	indexer := informerFactory.Longhorn().V1beta2().SupportBundles().Informer().GetIndexer()
	err = indexer.Add(supportBundle)
	c.Assert(err, IsNil)

	return supportBundle
}

func newSupportBundle(name, image, ip, currentOwnerID string, state longhorn.SupportBundleState, deletionTimeStamp *metav1.Time) *longhorn.SupportBundle {
	return &longhorn.SupportBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         TestNamespace,
			DeletionTimestamp: deletionTimeStamp,
		},
		Spec: longhorn.SupportBundleSpec{
			NodeID:      currentOwnerID,
			IssueURL:    "",
			Description: "",
		},
		Status: longhorn.SupportBundleStatus{
			OwnerID: currentOwnerID,
			Image:   image,
			IP:      ip,
			State:   state,
		},
	}
}

func fakeSupportBundleNamespace(c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Core().V1().Namespaces().Informer().GetIndexer()
	namespace, err := client.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestNamespace,
		},
	}, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = indexer.Add(namespace)
	c.Assert(err, IsNil)
}

func fakeSupportBundleManagerPod(name string, supportBundle *longhorn.SupportBundle, ds *datastore.DataStore, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	pod := newPod(
		&corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Ready: true}}},
		name+"-"+supportBundle.Name, TestNamespace,
		supportBundle.Status.OwnerID,
	)
	pod.Labels = ds.GetSupportBundleManagerLabel(supportBundle)
	pod.Status.PodIP = TestSupportBundleManagerPodIP
	pod, err := client.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	indexer := informerFactory.Core().V1().Pods().Informer().GetIndexer()
	err = indexer.Add(pod)
	c.Assert(err, IsNil)
}

func fakeSupportBundleFailedHistoryLimitSetting(value string, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	setting, err := client.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameSupportBundleFailedHistoryLimit),
		},
		Value: value,
	}, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = indexer.Add(setting)
	c.Assert(err, IsNil)
}

func fakeSupportBundleManagerImageSetting(c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	setting, err := client.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameSupportBundleManagerImage),
		},
		Value: "support-bundle-image:v0.0.0",
	}, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = indexer.Add(setting)
	c.Assert(err, IsNil)
}

type fakeSupportBundleHTTPClient struct {
}

func (m *fakeSupportBundleHTTPClient) Do(req *http.Request) (*http.Response, error) {
	status := &SupportBundleManagerStatus{
		Phase:        SupportBundleManagerPhase("done"),
		Error:        false,
		ErrorMessage: "",
		Progress:     100,
		FileName:     "supportbundle_08ccc085-641c-4592-bb57-e05456241204_2022-11-14T10-40-05Z.zip",
		FileSize:     834183,
	}
	statusByte, err := json.Marshal(status)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal from mocked client")
	}
	if req.URL.Path == "/status" {
		responseBody := io.NopCloser(bytes.NewBuffer(statusByte))
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       responseBody,
		}, nil
	}
	return &http.Response{}, nil
}
