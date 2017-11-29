# Longhorn Flexvolume Driver Deployment

Manager for Longhorn

## Requirement

1. Docker v1.13+
2. Kubernetes v1.8+
1. Make sure Longhorn for K8s has been deployment.
2. Make sure `jq` has been installed in the all nodes in kubernetes.
3. Make sure `findmnt` has been installed in the all nodes in kubernetes.
4. Make sure `curl` has been installed in the all nodes in kubernetes.

## Building
1. Go the dir `driver/build`
2. Exec the command `docker build . -t longhorn-volume-driver` to generate the image.
3. Push the image to your docker hub repository or your own private repository.

## Deployment
1. Go the dir `driver/deployment`.
2. Exec `kubectl get svc` find the service name for longhorn backend.
3. Fill the environment variable `LONGHORN_BACKEND_SVC` in `volume-driver-deploy.yaml` with the service name you get in the previous step.
4. Fill the name of `image` with it you build.
5. Exec `kubectl create -f volume-driver-deploy.yaml`.

## Test
1. Go the dir `driver/test`.
2. Exec `kubectl create -f test.yaml` to create the pod.
3. Exec `kubectl describe pod volume-test`, you will see the volume has been mounted successfully.
4. Exec `kubectl get pods`, you will find the pod for longhorn volume has been built.