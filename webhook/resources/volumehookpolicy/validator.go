package volumehookpolicy

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"

	admissionregv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/hookexecutor/snapshothook"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

// volumeHookPolicyValidator checks everything about a policy that does not
// depend on cluster state. Matching pods to hooks depends on which pods are
// running at pause time, so the executor checks that instead.
type volumeHookPolicyValidator struct {
	admission.DefaultValidator
	longhornNamespace string
}

func NewValidator(ds *datastore.DataStore) admission.Validator {
	return &volumeHookPolicyValidator{longhornNamespace: ds.Namespace()}
}

func (v *volumeHookPolicyValidator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "volumehookpolicies",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.VolumeHookPolicy{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (v *volumeHookPolicyValidator) Create(request *admission.Request, newObj runtime.Object) error {
	policy, ok := newObj.(*longhorn.VolumeHookPolicy)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeHookPolicy", newObj), "")
	}
	return v.validate(policy)
}

// Update re-validates the whole spec. Edits are never rejected for being
// in use: each SnapshotGroup records the policy generation it was admitted
// with, and fails with PolicyChanged if the policy changes after that.
func (v *volumeHookPolicyValidator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) error {
	policy, ok := newObj.(*longhorn.VolumeHookPolicy)
	if !ok {
		return werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.VolumeHookPolicy", newObj), "")
	}
	return v.validate(policy)
}

// validate rejects any policy in the Longhorn namespace. A policy applies
// only to PVCs in its own namespace, so this keeps hooks from ever running
// against Longhorn's own pods.
func (v *volumeHookPolicyValidator) validate(policy *longhorn.VolumeHookPolicy) error {
	if policy.Namespace == v.longhornNamespace {
		return werror.NewInvalidError(snapshothook.LonghornNamespaceMessage(v.longhornNamespace), "metadata.namespace")
	}
	return validateSpec(&policy.Spec)
}

func validateSpec(spec *longhorn.VolumeHookPolicySpec) error {
	if err := validatePVCScope(spec); err != nil {
		return err
	}

	if len(spec.Hooks) == 0 {
		return werror.NewInvalidError("hooks must have at least one entry", "spec.hooks")
	}

	// A policy with more than one hook requires a name and a podSelector
	// on every hook, so each pod matches exactly one of them. A single
	// hook may omit both: it matches every pod and is recorded under the
	// name "default".
	multiHook := len(spec.Hooks) > 1
	seenNames := map[string]int{}
	for i := range spec.Hooks {
		hook := &spec.Hooks[i]
		field := fmt.Sprintf("spec.hooks[%d]", i)

		if hook.Name != "" {
			if errs := validation.IsValidLabelValue(hook.Name); len(errs) > 0 {
				return werror.NewInvalidError(fmt.Sprintf("hook name %q is invalid: %v", hook.Name, strings.Join(errs, "; ")), field+".name")
			}
			if previous, dup := seenNames[hook.Name]; dup {
				return werror.NewInvalidError(fmt.Sprintf("hook name %q is used by hooks[%d] and hooks[%d]; names must be unique", hook.Name, previous, i), field+".name")
			}
			seenNames[hook.Name] = i
		} else if multiHook {
			return werror.NewInvalidError("every hook needs a name when the policy has more than one hook", field+".name")
		}

		if err := validateOperations(hook.Operations); err != nil {
			return werror.NewInvalidError(fmt.Sprintf("operations %v", err), field+".operations")
		}

		if hook.Exec == nil {
			return werror.NewInvalidError("exec is required", field+".exec")
		}
		if err := validateExec(hook.Exec, multiHook, field+".exec"); err != nil {
			return err
		}
	}

	return nil
}

// validateOperations rejects unknown and duplicate operations. The CRD
// schema enforces the same rules, so the API server catches both even when
// this webhook is unavailable; checking here keeps the webhook
// self-contained.
func validateOperations(operations []longhorn.HookOperation) error {
	seen := map[longhorn.HookOperation]int{}
	for i, operation := range operations {
		switch operation {
		case longhorn.HookOperationSnapshot, longhorn.HookOperationBackup:
		default:
			return fmt.Errorf("[%d] %q is not one of %q, %q", i, operation, longhorn.HookOperationSnapshot, longhorn.HookOperationBackup)
		}
		if previous, dup := seen[operation]; dup {
			return fmt.Errorf("[%d] %q is already listed at [%d]; entries must be unique", i, operation, previous)
		}
		seen[operation] = i
	}
	return nil
}

func validateExec(exec *longhorn.VolumeHookPolicyExec, multiHook bool, field string) error {
	if exec.PodSelector != nil {
		if err := validateLabelSelector(exec.PodSelector); err != nil {
			return werror.NewInvalidError(fmt.Sprintf("podSelector %v", err), field+".podSelector")
		}
	} else if multiHook {
		return werror.NewInvalidError("every hook needs a podSelector when the policy has more than one hook, so each pod matches exactly one", field+".podSelector")
	}

	if exec.Container != "" {
		if errs := validation.IsDNS1123Label(exec.Container); len(errs) > 0 {
			return werror.NewInvalidError(fmt.Sprintf("container name %q is invalid: %v", exec.Container, strings.Join(errs, "; ")), field+".container")
		}
	}

	if err := validateCommand(exec.PreCommand); err != nil {
		return werror.NewInvalidError(fmt.Sprintf("preCommand %v", err), field+".preCommand")
	}
	if err := validateCommand(exec.PostCommand); err != nil {
		return werror.NewInvalidError(fmt.Sprintf("postCommand %v", err), field+".postCommand")
	}
	return nil
}

// validatePVCScope requires exactly one of pvcSelector and pvcNames. With
// both set it is unclear which wins; with neither set the policy covers
// nothing, and an omitted selector must not silently match everything. A
// CEL rule in the CRD enforces the same requirement when this webhook is
// unavailable. The schema cannot validate the selector's contents or the
// PVC names, so that part is checked only here.
func validatePVCScope(spec *longhorn.VolumeHookPolicySpec) error {
	hasSelector := spec.PVCSelector != nil
	hasNames := len(spec.PVCNames) > 0
	switch {
	case hasSelector && hasNames:
		return werror.NewInvalidError("pvcSelector and pvcNames are mutually exclusive; set exactly one", "spec")
	case !hasSelector && !hasNames:
		return werror.NewInvalidError("exactly one of pvcSelector and pvcNames must be set", "spec")
	case hasSelector:
		if err := validateLabelSelector(spec.PVCSelector); err != nil {
			return werror.NewInvalidError(fmt.Sprintf("pvcSelector %v", err), "spec.pvcSelector")
		}
		return nil
	}

	seen := map[string]int{}
	for i, name := range spec.PVCNames {
		field := fmt.Sprintf("spec.pvcNames[%d]", i)
		if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
			return werror.NewInvalidError(fmt.Sprintf("PVC name %q is invalid: %v", name, strings.Join(errs, "; ")), field)
		}
		if previous, dup := seen[name]; dup {
			return werror.NewInvalidError(fmt.Sprintf("PVC name %q is listed at pvcNames[%d] and pvcNames[%d]; names must be unique", name, previous, i), field)
		}
		seen[name] = i
	}
	return nil
}

// validateLabelSelector requires a selector that parses and has at least
// one match entry, because Kubernetes treats an empty selector as matching
// everything.
func validateLabelSelector(selector *metav1.LabelSelector) error {
	if len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0 {
		return fmt.Errorf("must have at least one matchLabels or matchExpressions entry")
	}
	if _, err := metav1.LabelSelectorAsSelector(selector); err != nil {
		return fmt.Errorf("is invalid: %v", err)
	}
	return nil
}

// validateCommand requires a command whose first element, the program to
// run, is non-empty. The command only has meaning inside the pod that runs
// it, so nothing more can be checked at admission.
func validateCommand(command []string) error {
	if len(command) == 0 {
		return fmt.Errorf("must have at least one element")
	}
	if strings.TrimSpace(command[0]) == "" {
		return fmt.Errorf("first element is the program to run and must not be empty")
	}
	return nil
}
