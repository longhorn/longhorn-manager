# Admission Webhook

The admission webhook only validates or mutates the spec of hooked resources
before being persistent to the database like etcd. Any resource reconciliation
to ensure resources behave as expected should be handled in the backend as usual.
For example, controllers or even host operations like creating and deleting
folders at the host as usual.

Most of CRDs in the uninstallation are deleted without checks. Thus, the validation
logics for DELETE operations should be added carefully to prevent from the
uninstallation failure.

Reference: https://github.com/longhorn/longhorn-manager/pull/1279

