package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// snapshotGroupPreviewPodReadTimeout caps the total time one preview spends
// reading pods, because a user is waiting on the response.
const snapshotGroupPreviewPodReadTimeout = 10 * time.Second

// SnapshotGroupAction dispatches POST /v1/snapshotgroups by the action query
// parameter. A POST without an action is the conventional create request;
// unknown actions are rejected so a mistyped action cannot fall through to
// create.
func (s *Server) SnapshotGroupAction(rw http.ResponseWriter, req *http.Request) error {
	switch action := req.URL.Query().Get("action"); action {
	case "":
		return s.SnapshotGroupCreate(rw, req)
	case "preview":
		return s.SnapshotGroupPreview(rw, req)
	default:
		writeErr(rw, req, fmt.Errorf("unknown snapshot group action %q; supported action: preview", action), http.StatusBadRequest)
		return nil
	}
}

func (s *Server) SnapshotGroupList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	sgl, err := s.snapshotGroupList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(sgl)
	return nil
}

func (s *Server) snapshotGroupList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListSnapshotGroupsSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list snapshot groups")
	}
	return toSnapshotGroupCollection(list, apiContext), nil
}

func (s *Server) SnapshotGroupGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	snapshotGroup, err := s.m.GetSnapshotGroup(id)
	if err != nil {
		return errors.Wrapf(err, "failed to get snapshot group %v", id)
	}
	apiContext.Write(toSnapshotGroupResource(snapshotGroup, apiContext))
	return nil
}

func (s *Server) SnapshotGroupCreate(rw http.ResponseWriter, req *http.Request) error {
	var input SnapshotGroup
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	obj, err := s.m.CreateSnapshotGroup(input.Name, &longhorn.SnapshotGroupSpec{
		Volumes:         input.Volumes,
		VolumeSelector:  input.VolumeSelector,
		Labels:          input.Labels,
		DeadlineSeconds: input.DeadlineSeconds,
		Hooks:           input.Hooks,
		HookOperation:   input.HookOperation,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to create snapshot group %v", input.Name)
	}
	apiContext.Write(toSnapshotGroupResource(obj, apiContext))
	return nil
}

// SnapshotGroupPreview previews which volumes a snapshot group spec would
// select, without creating the group, and with hooks command also the
// policy and running pods of each member. Members are listed in pause order.
// Selection failures are reported in the preview body, not as HTTP errors.
func (s *Server) SnapshotGroupPreview(rw http.ResponseWriter, req *http.Request) error {
	var input SnapshotGroupPreviewInput
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	preview := &SnapshotGroupPreviewOutput{
		Resource: client.Resource{
			Type: "snapshotGroupPreviewOutput",
		},
	}
	candidates, err := s.m.ResolveSnapshotGroupMemberCandidates(&longhorn.SnapshotGroupSpec{
		Volumes:        input.Volumes,
		VolumeSelector: input.VolumeSelector,
	})
	if err != nil {
		preview.Error = err.Error()
	}
	orderSnapshotGroupCandidatesForPause(candidates, input.Volumes)
	operation := snapshothook.EffectiveHookOperation(input.HookOperation)
	hooked := snapshothook.HooksEnabled(input.Hooks)
	if hooked && preview.Error == "" {
		// Admission rejects hooked groups until every manager pod runs the
		// new image. The preview reports the same error, so a preview that
		// passes means the create will not be rejected for this reason.
		outdated, err := s.m.ListOutdatedManagerPods()
		if err != nil {
			return errors.Wrap(err, "failed to check the longhorn-manager rollout")
		}
		if len(outdated) > 0 {
			preview.Error = datastore.OutdatedManagerPodsMessage(outdated)
		}
	}
	podReadCtx, cancelPodReads := context.WithTimeout(req.Context(), snapshotGroupPreviewPodReadTimeout)
	defer cancelPodReads()
	for _, candidate := range candidates {
		member := SnapshotGroupPreviewMember{
			VolumeName:        candidate.VolumeName,
			ValidationFailure: candidate.ValidationFailure,
		}
		if hooked {
			hook, err := s.m.PreviewSnapshotGroupMemberHook(candidate.VolumeName, operation)
			if err != nil {
				return errors.Wrapf(err, "failed to preview the snapshot hook of volume %v", candidate.VolumeName)
			}
			member.PVCRef = hook.PVCRef
			member.HookPolicyRef = hook.HookPolicyRef
			if hook.PVCRef != nil {
				// A volume without a PVC has no workload pods.
				member.WorkloadPods = describeSnapshotGroupPreviewPods(podReadCtx, hook.PVCRef.Namespace, hook.WorkloadPods, s.m.GetWorkloadPod, hook.HookPolicy, operation)
			}
			if member.ValidationFailure == "" {
				member.ValidationFailure = hook.ValidationFailure
				member.ValidationFailureReason = hook.ValidationFailureReason
			}
			preview.Warnings = append(preview.Warnings, hook.Warnings...)
		}
		preview.Members = append(preview.Members, member)
	}
	if hooked {
		recordHookExecutorAccess(preview.Members, s.m.VerifyHookExecutorAccess)
		preview.Namespaces, err = describeSnapshotGroupPreviewNamespaces(preview.Members, s.m.ListVolumeHookPolicies)
		if err != nil {
			return errors.Wrap(err, "failed to list the volume hook policies of the member namespaces")
		}
	}
	preview.Warnings = append(preview.Warnings, snapshotGroupPreviewWarnings(preview.Members)...)
	apiContext.Write(preview)
	return nil
}

// describeSnapshotGroupPreviewNamespaces lists the hook policies of every
// namespace the members' PVCs live in, once per namespace, sorted by
// namespace. A namespace without a policy is kept, with an empty list.
func describeSnapshotGroupPreviewNamespaces(members []SnapshotGroupPreviewMember, listPolicies func(namespace string) ([]*longhorn.VolumeHookPolicy, error)) ([]SnapshotGroupPreviewNamespace, error) {
	namespaces := []SnapshotGroupPreviewNamespace{}
	seen := map[string]bool{}
	for _, member := range members {
		if member.PVCRef == nil || seen[member.PVCRef.Namespace] {
			continue
		}
		namespace := member.PVCRef.Namespace
		seen[namespace] = true
		policies, err := listPolicies(namespace)
		if err != nil {
			return nil, errors.Wrapf(err, "namespace %v", namespace)
		}
		namespaces = append(namespaces, SnapshotGroupPreviewNamespace{
			Namespace:    namespace,
			HookPolicies: toVolumeHookPolicyResources(policies),
		})
	}
	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i].Namespace < namespaces[j].Namespace
	})
	return namespaces, nil
}

// describeSnapshotGroupPreviewPods describes each of a member's running
// pods for the preview. A pod that cannot be read keeps its name with the
// reason in ReadFailure. Pods are read one at a time under the caller's
// deadline.
func describeSnapshotGroupPreviewPods(ctx context.Context, namespace string, names []string, getPod func(ctx context.Context, namespace, name string) (*corev1.Pod, error), policy *longhorn.VolumeHookPolicy, operation longhorn.HookOperation) []SnapshotGroupPreviewPod {
	var pods []SnapshotGroupPreviewPod
	for _, name := range names {
		preview := SnapshotGroupPreviewPod{Name: name}
		pod, err := getPod(ctx, namespace, name)
		switch {
		case apierrors.IsNotFound(err):
			preview.ReadFailure = "pod no longer exists; the volume's workloadsStatus is stale, retry the preview"
		case err != nil:
			preview.ReadFailure = fmt.Sprintf("%v; retry the preview", err)
		default:
			preview.Labels = pod.Labels
			preview.Containers = snapshothook.HookContainerNames(pod)
			if policy != nil {
				hook, container, err := snapshothook.MatchHookForPod(policy, pod, operation)
				if err != nil {
					preview.MatchFailure = err.Error()
				} else {
					preview.HookName = snapshothook.HookName(hook)
					preview.Container = container
				}
			}
		}
		pods = append(pods, preview)
	}
	return pods
}

// recordHookExecutorAccess runs the access checks admission runs and
// records the result in each member's AccessFailure and
// AccessFailureManifest. Members already failing another check are checked
// too, so a namespace's missing grant shows next to its missing policy and
// the user can add both at once.
func recordHookExecutorAccess(members []SnapshotGroupPreviewMember, verify func(namespace string) (*hookexecutor.AccessDenial, error)) {
	type accessReview struct {
		failure, manifest string
	}
	results := map[string]accessReview{}
	for i := range members {
		member := &members[i]
		if member.PVCRef == nil {
			continue
		}
		namespace := member.PVCRef.Namespace
		result, reviewed := results[namespace]
		if !reviewed {
			denial, err := verify(namespace)
			switch {
			case err != nil:
				result.failure = fmt.Sprintf("%v; retry the preview", err)
			case denial != nil:
				result.failure = denial.Reason
				result.manifest = denial.Manifest
			}
			results[namespace] = result
		}
		member.AccessFailure = result.failure
		member.AccessFailureManifest = result.manifest
	}
}

// orderSnapshotGroupCandidatesForPause reorders the name-sorted candidates
// into the order the executor would pause them.
func orderSnapshotGroupCandidatesForPause(candidates []datastore.SnapshotGroupMemberCandidate, volumes []string) {
	snapshothook.SortByPauseOrder(volumes, candidates, func(candidate datastore.SnapshotGroupMemberCandidate) string {
		return candidate.VolumeName
	})
}

// snapshotGroupPreviewWarnings warns about conditions admission accepts but
// that cause problems at run time: a pod that matches no hook of its
// member's policy, and a pod mounted by more than one member.
func snapshotGroupPreviewWarnings(members []SnapshotGroupPreviewMember) []string {
	var warnings []string
	var podKeys []string
	membersByPod := map[string][]string{}
	for _, member := range members {
		if member.PVCRef == nil {
			// No PVC, no workload pods.
			continue
		}
		for _, pod := range member.WorkloadPods {
			key := member.PVCRef.Namespace + "/" + pod.Name
			if pod.MatchFailure != "" {
				warnings = append(warnings, fmt.Sprintf("pod %v: %v; the run would fail member volume %v with HookNotMatched", key, pod.MatchFailure, member.VolumeName))
			}
			if _, seen := membersByPod[key]; !seen {
				podKeys = append(podKeys, key)
			}
			membersByPod[key] = append(membersByPod[key], member.VolumeName)
		}
	}
	for _, key := range podKeys {
		if volumes := membersByPod[key]; len(volumes) > 1 {
			warnings = append(warnings, fmt.Sprintf("pod %v mounts member volumes %v; its hooks run once per member, each run counting against the deadline", key, strings.Join(volumes, ", ")))
		}
	}
	return warnings
}

func (s *Server) SnapshotGroupDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteSnapshotGroup(id); err != nil {
		return errors.Wrapf(err, "failed to delete snapshot group %v", id)
	}

	return nil
}
