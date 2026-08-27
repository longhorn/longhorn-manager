package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rancher/go-rancher/client"

	rancherapi "github.com/rancher/go-rancher/api"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lhclient "github.com/longhorn/longhorn-manager/client"
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

func TestVolumeSchemaAllowsLocalProvisioningModeOnCreate(t *testing.T) {
	volumeSchema := NewSchema().Schema("volume")
	field := volumeSchema.Field("localProvisioningMode")
	if !field.Create {
		t.Fatal("expected localProvisioningMode to be accepted when creating a volume")
	}
}

func TestToVolumeResourceUsesOldSizeUntilLiveFrontendCatchesUp(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost/v1/volumes/test-volume", nil)
	urlBuilder, err := rancherapi.NewUrlBuilder(req, &client.Schemas{})
	if err != nil {
		t.Fatalf("failed to create API url builder: %v", err)
	}

	volume := &longhorn.Volume{}
	volume.Name = "test-volume"
	volume.Spec.DataEngine = longhorn.DataEngineTypeV2
	volume.Spec.Size = 20
	volume.Spec.NumberOfReplicas = 1
	volume.Status.State = longhorn.VolumeStateAttached

	engine := &longhorn.Engine{}
	engine.Name = "test-volume-e-0"
	engine.Spec.VolumeName = volume.Name
	engine.Spec.NodeID = "engine-node"
	engine.Spec.Image = "ei-test"
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.CurrentSize = 20
	engine.Status.IP = "10.0.0.2"

	frontend := &longhorn.EngineFrontend{}
	frontend.Name = "test-volume-ef-0"
	frontend.Spec.EngineName = engine.Name
	frontend.Spec.NodeID = "frontend-node"
	frontend.Status.CurrentState = longhorn.InstanceStateRunning
	frontend.Status.Endpoint = "/dev/longhorn/test-volume"
	frontend.Status.CurrentSize = 10

	resource := toVolumeResource(volume, []*longhorn.EngineFrontend{frontend}, []*longhorn.Engine{engine}, nil, nil, nil, &rancherapi.ApiContext{UrlBuilder: urlBuilder})
	if got := resource.Controllers[0].Size; got != "10" {
		t.Fatalf("expected controller size to stay at live frontend size, got %q", got)
	}
}

func TestToVolumeResourceUsesEngineSizeWhenFrontendIsNotLive(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost/v1/volumes/test-volume", nil)
	urlBuilder, err := rancherapi.NewUrlBuilder(req, &client.Schemas{})
	if err != nil {
		t.Fatalf("failed to create API url builder: %v", err)
	}

	volume := &longhorn.Volume{}
	volume.Name = "test-volume"
	volume.Spec.DataEngine = longhorn.DataEngineTypeV2
	volume.Spec.Size = 20
	volume.Spec.NumberOfReplicas = 1
	volume.Status.State = longhorn.VolumeStateAttached

	engine := &longhorn.Engine{}
	engine.Name = "test-volume-e-0"
	engine.Spec.VolumeName = volume.Name
	engine.Spec.NodeID = "engine-node"
	engine.Spec.Image = "ei-test"
	engine.Status.CurrentState = longhorn.InstanceStateRunning
	engine.Status.CurrentSize = 20
	engine.Status.IP = "10.0.0.2"

	frontend := &longhorn.EngineFrontend{}
	frontend.Name = "test-volume-ef-0"
	frontend.Spec.EngineName = engine.Name
	frontend.Spec.NodeID = "frontend-node"
	frontend.Status.CurrentState = longhorn.InstanceStateStopped
	frontend.Status.Endpoint = ""
	frontend.Status.CurrentSize = 10

	resource := toVolumeResource(volume, []*longhorn.EngineFrontend{frontend}, []*longhorn.Engine{engine}, nil, nil, nil, &rancherapi.ApiContext{UrlBuilder: urlBuilder})
	if got := resource.Controllers[0].Size; got != "20" {
		t.Fatalf("expected controller size to use engine size when frontend is not live, got %q", got)
	}
}

func TestToVolumeResourcePropagatesReadyMessage(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://localhost/v1/volumes/test-volume", nil)
	urlBuilder, err := rancherapi.NewUrlBuilder(req, &client.Schemas{})
	if err != nil {
		t.Fatalf("failed to create API url builder: %v", err)
	}

	volume := &longhorn.Volume{}
	volume.Name = "test-volume"
	volume.Spec.NodeID = "test-node"
	volume.Status.Robustness = longhorn.VolumeRobustnessFaulted

	resource := toVolumeResource(volume, nil, nil, nil, nil, nil, &rancherapi.ApiContext{UrlBuilder: urlBuilder})
	if resource.Ready {
		t.Fatalf("expected volume to not be ready")
	}
	if resource.NotReadyMessage == "" {
		t.Fatalf("expected NotReadyMessage to be populated when volume is not ready")
	}
}

// Age-based retention (longhorn/longhorn#12060) is only usable if retainAge is
// settable through the API the same way retain is. If the schema stops
// advertising it as creatable, clients that build requests from the schema
// (the UI and the Python client) silently drop the field and the job falls
// back to count-only retention.
func TestRecurringJobSchemaAllowsSettingRetainAge(t *testing.T) {
	schemas := NewSchema()
	job, ok := schemas.CheckSchema("recurringJob")
	if !ok {
		t.Fatalf("expected recurringJob schema to be registered")
	}

	retain, ok := job.CheckField("retain")
	if !ok {
		t.Fatalf("expected recurringJob schema to have a retain field")
	}

	retainAge, ok := job.CheckField("retainAge")
	if !ok {
		t.Fatalf("expected recurringJob schema to have a retainAge field")
	}
	if retainAge.Create != retain.Create {
		t.Fatalf("expected retainAge to be creatable like retain, got create=%v", retainAge.Create)
	}
	// metav1.Duration reflects to the "v1.Duration" struct type, which is not a
	// registered schema; it must be described as the duration string it is on
	// the wire, otherwise the generated client cannot type the field.
	if retainAge.Type != "string" {
		t.Fatalf("expected retainAge schema type to be string, got %q", retainAge.Type)
	}
}

// The Go client carries retainAge as a string while the manager stores it as a
// metav1.Duration. A caller setting RetainAge must end up with that exact
// window on the spec, otherwise the recurring job would clean up on the wrong
// schedule.
func TestRecurringJobRetainAgeRoundTripsFromClient(t *testing.T) {
	body, err := json.Marshal(&lhclient.RecurringJob{
		Name:      "test-job",
		Task:      string(longhorn.RecurringJobTypeSnapshot),
		Cron:      "*/1 * * * *",
		Retain:    50,
		RetainAge: "10m",
	})
	if err != nil {
		t.Fatalf("failed to marshal client recurring job: %v", err)
	}

	var input RecurringJob
	if err := json.Unmarshal(body, &input); err != nil {
		t.Fatalf("failed to decode client request into API model: %v", err)
	}
	if input.RetainAge.Duration != 10*time.Minute {
		t.Fatalf("expected retainAge 10m, got %v", input.RetainAge.Duration)
	}
	if input.Retain != 50 {
		t.Fatalf("expected retain 50, got %d", input.Retain)
	}

	// An unset RetainAge must stay zero so age-based retention remains off.
	body, err = json.Marshal(&lhclient.RecurringJob{Name: "test-job", Retain: 50})
	if err != nil {
		t.Fatalf("failed to marshal client recurring job: %v", err)
	}
	input = RecurringJob{}
	if err := json.Unmarshal(body, &input); err != nil {
		t.Fatalf("failed to decode client request into API model: %v", err)
	}
	if input.RetainAge.Duration != 0 {
		t.Fatalf("expected retainAge to stay zero when unset, got %v", input.RetainAge.Duration)
	}
}

// Go durations have no day or year unit, so "1d" and "1y" cannot be accepted.
// They must fail at decode — and therefore at admission — rather than being
// silently read as zero, which would leave age-based retention quietly off on a
// job the user believes is configured.
func TestRecurringJobRetainAgeRejectsDayAndYearUnits(t *testing.T) {
	for _, retainAge := range []string{"1d", "1y", "7 days", "abc"} {
		t.Run(retainAge, func(t *testing.T) {
			body, err := json.Marshal(&lhclient.RecurringJob{Name: "test-job", RetainAge: retainAge})
			if err != nil {
				t.Fatalf("failed to marshal client recurring job: %v", err)
			}

			var input RecurringJob
			if err := json.Unmarshal(body, &input); err == nil {
				t.Fatalf("expected retainAge %q to be rejected, got %v", retainAge, input.RetainAge.Duration)
			}
		})
	}
}

// toRecurringJobResource is what the UI and client read back; dropping
// retainAge there would make a set window invisible after a GET.
func TestToRecurringJobResourceIncludesRetainAge(t *testing.T) {
	retainAge := metav1.Duration{Duration: 10 * time.Minute}

	recurringJob := &longhorn.RecurringJob{}
	recurringJob.Name = "test-job"
	recurringJob.Spec.Retain = 50
	recurringJob.Spec.RetainAge = retainAge

	resource := toRecurringJobResource(recurringJob, nil)
	if resource.RetainAge.Duration != retainAge.Duration {
		t.Fatalf("expected retainAge %v, got %v", retainAge.Duration, resource.RetainAge.Duration)
	}
}

// The retention policy decides whether the job cleans up by retain or by
// retainAge, so a client that cannot set it is stuck on the count-based default
// and the age window it sends is never read. The schema is what the UI and the
// Python client build requests from, so the field has to be advertised as
// creatable there like retain is.
func TestRecurringJobSchemaAllowsSettingRetentionPolicy(t *testing.T) {
	schemas := NewSchema()
	job, ok := schemas.CheckSchema("recurringJob")
	if !ok {
		t.Fatalf("expected recurringJob schema to be registered")
	}

	retain, ok := job.CheckField("retain")
	if !ok {
		t.Fatalf("expected recurringJob schema to have a retain field")
	}

	retentionPolicy, ok := job.CheckField("retentionPolicy")
	if !ok {
		t.Fatalf("expected recurringJob schema to have a retentionPolicy field")
	}
	if retentionPolicy.Create != retain.Create {
		t.Fatalf("expected retentionPolicy to be creatable like retain, got create=%v", retentionPolicy.Create)
	}
	if retentionPolicy.Type != "string" {
		t.Fatalf("expected retentionPolicy schema type to be string, got %q", retentionPolicy.Type)
	}
}

// A client asking for "age-based" must get "age-based" on the spec. Losing the
// value in decode would leave the job on count-based, quietly ignoring the
// retainAge the caller sent in the same request and retaining by a count they
// never meant to rely on.
func TestRecurringJobRetentionPolicyRoundTripsFromClient(t *testing.T) {
	body, err := json.Marshal(&lhclient.RecurringJob{
		Name:            "test-job",
		Task:            string(longhorn.RecurringJobTypeSnapshot),
		Cron:            "*/1 * * * *",
		Retain:          50,
		RetainAge:       "10m",
		RetentionPolicy: string(longhorn.RecurringJobRetentionPolicyAgeBased),
	})
	if err != nil {
		t.Fatalf("failed to marshal client recurring job: %v", err)
	}

	var input RecurringJob
	if err := json.Unmarshal(body, &input); err != nil {
		t.Fatalf("failed to decode client request into API model: %v", err)
	}
	if input.RetentionPolicy != longhorn.RecurringJobRetentionPolicyAgeBased {
		t.Fatalf("expected retentionPolicy %v, got %v", longhorn.RecurringJobRetentionPolicyAgeBased, input.RetentionPolicy)
	}

	// An unset policy must stay empty rather than being guessed at here; the CRD
	// default fills it in, and filterExpiredItems reads empty as "count-based".
	body, err = json.Marshal(&lhclient.RecurringJob{Name: "test-job", Retain: 50})
	if err != nil {
		t.Fatalf("failed to marshal client recurring job: %v", err)
	}
	input = RecurringJob{}
	if err := json.Unmarshal(body, &input); err != nil {
		t.Fatalf("failed to decode client request into API model: %v", err)
	}
	if input.RetentionPolicy != "" {
		t.Fatalf("expected retentionPolicy to stay empty when unset, got %q", input.RetentionPolicy)
	}
}

// toRecurringJobResource is what the UI reads back. If the policy is dropped
// there, an "age-based" job is indistinguishable from a "count-based" one in the
// UI, and both retain and retainAge are shown as if they were in force when only
// one of them ever is.
func TestToRecurringJobResourceIncludesRetentionPolicy(t *testing.T) {
	recurringJob := &longhorn.RecurringJob{}
	recurringJob.Name = "test-job"
	recurringJob.Spec.Retain = 50
	recurringJob.Spec.RetainAge = metav1.Duration{Duration: 10 * time.Minute}
	recurringJob.Spec.RetentionPolicy = longhorn.RecurringJobRetentionPolicyAgeBased

	resource := toRecurringJobResource(recurringJob, nil)
	if resource.RetentionPolicy != longhorn.RecurringJobRetentionPolicyAgeBased {
		t.Fatalf("expected retentionPolicy %v, got %v", longhorn.RecurringJobRetentionPolicyAgeBased, resource.RetentionPolicy)
	}
}
