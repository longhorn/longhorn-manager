package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rancher/go-rancher/client"

	rancherapi "github.com/rancher/go-rancher/api"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestToVolumeResourceUsesEngineFrontendNodeForV2Controller(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost/v1/volumes/test-volume", nil)
	urlBuilder, err := rancherapi.NewUrlBuilder(req, &client.Schemas{})
	if err != nil {
		t.Fatalf("failed to create API url builder: %v", err)
	}

	volume := &longhorn.Volume{}
	volume.Name = "test-volume"
	volume.Spec.DataEngine = longhorn.DataEngineTypeV2
	volume.Spec.Size = 10
	volume.Spec.NumberOfReplicas = 1

	engine := &longhorn.Engine{}
	engine.Name = "test-volume-e-0"
	engine.Spec.VolumeName = volume.Name
	engine.Spec.NodeID = "engine-node"
	engine.Spec.Image = "ei-test"
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.CurrentSize = 10
	engine.Status.IP = "10.0.0.2"

	frontend := &longhorn.EngineFrontend{}
	frontend.Name = "test-volume-ef-0"
	frontend.Spec.EngineName = engine.Name
	frontend.Spec.NodeID = "frontend-node"
	frontend.Status.Endpoint = "/dev/longhorn/test-volume"

	resource := toVolumeResource(volume, []*longhorn.EngineFrontend{frontend}, []*longhorn.Engine{engine}, nil, nil, nil, &rancherapi.ApiContext{UrlBuilder: urlBuilder})
	if len(resource.Controllers) != 1 {
		t.Fatalf("expected one controller, got %d", len(resource.Controllers))
	}

	if resource.Controllers[0].NodeID != "frontend-node" {
		t.Fatalf("expected controller hostId to use frontend node, got %q", resource.Controllers[0].NodeID)
	}
	if resource.Controllers[0].Endpoint != "/dev/longhorn/test-volume" {
		t.Fatalf("expected controller endpoint to use frontend endpoint, got %q", resource.Controllers[0].Endpoint)
	}
}
