# HelixDB Kubernetes Deployment

This repository contains a **Helm chart** and a **Docker image** to deploy the `helixdb` binary as a containerized service on Kubernetes.

The deployment is designed to run in a **minimal Debian-based container** and uses a **Persistent Volume Claim** for data storage.

---

## Docker Image

The container image runs the the `helix-container` binary which was compiled with the helix push tool previously.

## Build and Push

The Helm chart is located in the helm/ directory and provides a configurable Kubernetes deployment, service, and persistent volume claim.

### Helm Chart Structure

The Helm chart is located in the `helm/` directory and provides a configurable Kubernetes deployment, service, and persistent volume claim.

```text
helm/
├── Chart.yaml          # Chart metadata
├── values.yaml         # Default configuration values
└── templates/
    ├── deployment.yaml # Deployment definition
    ├── pvc.yaml        # PersistentVolumeClaim definition
    └── service.yaml    # Service definition
```


## Deploying to Kubernetes

helm install helixdb ./helm -n your-namespace --create-namespace

## using an existing PVC

If you are using a previously created PVC, make sure it has accessModes of type ReadWriteOncePod and includes the following annotations:

  * app.kubernetes.io/managed-by=Helm
  *  meta.helm.sh/release-namespace=<namespace>
  * meta.helm.sh/release-name=helixdb 

##  Notes

The container uses non-root UID 1001 for security and OpenShift compatibility.

The HELIX_DATA_DIR environment variable points to /data, which is mounted via a PersistentVolumeClaim.

All configurations are customizable via values.yaml.
