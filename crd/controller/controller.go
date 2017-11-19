package controller

import (
	"strconv"
	"fmt"
	"k8s.io/client-go/tools/cache"
	"time"
	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/crdstore"
	"github.com/rancher/longhorn-manager/crd/crdtype"
	"github.com/rancher/longhorn-manager/types"
)

func RegisterVolumeController(m *manager.VolumeManager, ds *crdstore.CRDStore ) {
	_, controller := cache.NewInformer(
		ds.VolumeOperator.NewListWatch(),
		&crdtype.Crdvolume{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {

				crdVolume, ok := obj.(*crdtype.Crdvolume)
				if ok && crdVolume.Spec.TargetNodeID == "" {
					var lhVolume types.VolumeInfo
					crdtype.CRDVolume2LhVoulme(crdVolume, &lhVolume)
					m.CRDVolumeCreate(&lhVolume, crdVolume.ObjectMeta.Name)
				}

			},

			DeleteFunc: func(obj interface{}) {

				crdVolume, ok := obj.(*crdtype.Crdvolume)
				if ok {
					var lhVolume types.VolumeInfo
					crdtype.CRDVolume2LhVoulme(crdVolume, &lhVolume)
					m.CRDVolumeDelete(&lhVolume)
				}

			},

			UpdateFunc: func(oldObj, newObj interface{}) {

				newCrdVolume := newObj.(*crdtype.Crdvolume)
				oldCrdVolume, ok := oldObj.(*crdtype.Crdvolume)
				if ok && newCrdVolume.OperateFromKubectl{
					var oldVolume types.VolumeInfo
					var newVolume types.VolumeInfo
					crdtype.CRDVolume2LhVoulme(oldCrdVolume, &oldVolume)
					crdtype.CRDVolume2LhVoulme(newCrdVolume, &newVolume)

					//Use the ResourceVersion of newCrdvolume, because we need to update the new crd again
					kindex, err := strconv.ParseUint(newCrdVolume.ObjectMeta.ResourceVersion, 10, 64)
					if err != nil {
						fmt.Errorf("Parse index error")
						return
					}
					m.CRDVolumeAttachDetach(&oldVolume, &newVolume, kindex)
				}

			},
		},
	)

	stop := make(chan struct{})
	go controller.Run(stop)
}