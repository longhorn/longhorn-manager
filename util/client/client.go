package client

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/wrangler/pkg/clients"
	"github.com/rancher/wrangler/pkg/schemes"

	v1 "k8s.io/api/apps/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/longhorn/longhorn-manager/datastore"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

type Client struct {
	clients.Clients
	Datastore *datastore.DataStore
}

func NewClient(ctx context.Context, config *rest.Config, namespace string, needDataStore bool) (*Client, error) {
	if err := schemes.Register(v1.AddToScheme); err != nil {
		return nil, err
	}

	clients, err := clients.NewFromConfig(config, nil)
	if err != nil {
		return nil, err
	}

	var ds *datastore.DataStore

	if needDataStore {
		kubeClient, err := clientset.NewForConfig(config)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get k8s client")
		}

		extensionsClient, err := apiextensionsclientset.NewForConfig(config)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get k8s extension client")
		}

		lhClient, err := lhclientset.NewForConfig(config)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get lh client")
		}

		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
		lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, time.Second*30)

		ds = datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, extensionsClient, namespace)

		go kubeInformerFactory.Start(ctx.Done())
		go lhInformerFactory.Start(ctx.Done())

		if !ds.Sync(ctx.Done()) {
			return nil, fmt.Errorf("datastore cache sync up failed")
		}
	}

	return &Client{
		Clients:   *clients,
		Datastore: ds,
	}, nil
}
