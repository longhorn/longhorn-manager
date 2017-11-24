package crdcontroller

import (
	"fmt"
	"strconv"
	"time"

	"github.com/rancher/longhorn-manager/crd/crdtype"
	"github.com/rancher/longhorn-manager/crdstore"
	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/types"
	"k8s.io/client-go/tools/cache"
)

func RegisterVolumeController(m *manager.VolumeManager, datastore interface{}) {
	ds, ok := datastore.(*crdstore.CRDStore)
	if !ok {
		fmt.Errorf("Register volume controller failed")
		return
	}

	_, controller := cache.NewInformer(
		ds.Operator.NewListWatch(crdtype.CrdMap[crdtype.KeyVolume].CrdPlural),
		&crdtype.Volume{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				crdVolume, ok := obj.(*crdtype.Volume)
				if ok && crdVolume.OperateFromKubectl {
					var lhVolume types.VolumeInfo
					crdtype.CRDVolume2LhVolume(crdVolume, &lhVolume)
					if err := m.CRDVolumeCreate(&lhVolume, crdVolume.ObjectMeta.Name); err != nil {
						fmt.Errorf(err.Error())
					}
				}
			},

			DeleteFunc: func(obj interface{}) {
				crdVolume, ok := obj.(*crdtype.Volume)
				if ok {
					var lhVolume types.VolumeInfo
					crdtype.CRDVolume2LhVolume(crdVolume, &lhVolume)
					if err := m.CRDVolumeDelete(&lhVolume); err != nil {
						fmt.Errorf(err.Error())
					}
				}
			},

			UpdateFunc: func(oldObj, newObj interface{}) {
				newCrdVolume, ok := newObj.(*crdtype.Volume)
				if !ok {
					return
				}

				oldCrdVolume, ok := oldObj.(*crdtype.Volume)
				if ok && newCrdVolume.OperateFromKubectl {
					var oldVolume types.VolumeInfo
					var newVolume types.VolumeInfo
					crdtype.CRDVolume2LhVolume(oldCrdVolume, &oldVolume)
					crdtype.CRDVolume2LhVolume(newCrdVolume, &newVolume)

					//Use the ResourceVersion of newCrdvolume, because we need to update the new crd again
					kvIndex, err := strconv.ParseUint(newCrdVolume.ObjectMeta.ResourceVersion, 10, 64)
					if err != nil {
						fmt.Errorf(err.Error())
						return
					}

					if err := m.CRDVolumeAttachDetach(&oldVolume, &newVolume, kvIndex); err != nil {
						fmt.Errorf(err.Error())
					}

				}
			},
		},
	)

	stop := make(chan struct{})
	go controller.Run(stop)
}
