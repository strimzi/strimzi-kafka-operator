# Tekton Pipelines for Strimzi Kafka Operator System Tests

This directory contains Tekton pipeline definitions for running Strimzi Kafka Operator system tests.

## Prerequisites

- Tekton Pipelines installed on your cluster
- Container registry access for the test image
- Kubernetes cluster with sufficient resources
- Kubeconfig secret for target test cluster access

## Setup

### 1. Create a namespace for tekton

```bash
kubectl create namespace test
```

### 2. Create kubeconfig secret

Create a secret from your current kubeconfig:

```bash
kubectl create secret generic kubeconfig --from-file=config=$HOME/.kube/config
```

To use a different kubeconfig secret name:

```bash
# Create with custom name
kubectl create secret generic my-cluster-config --from-file=config=$HOME/.kube/config

# Reference it in the pipeline parameters
# - name: kubeconfig-secret
#   value: "my-cluster-config"
```

### 3. (Optional) Create environment variables ConfigMap

Create an ConfigMap with environment variables for your tests:

```bash
kubectl apply -f example-env-configmap.yaml
```

Or create your own ConfigMap with custom environment variables:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-test-env
data:
  env-vars: |
    KAFKA_TIERED_STORAGE_BASE_IMAGE=quay.io/strimzi/kafka:latest-kafka-4.0.0
    CLUSTER_OPERATOR_INSTALL_TYPE=Yaml
    BRIDGE_IMAGE=latest-released
    STRIMZI_LOG_LEVEL=DEBUG
```

### 4. Install Tekton resources

```bash
kubectl apply -f .tekton/task.yaml
kubectl apply -f .tekton/pipeline.yaml
```

### 5. Run the pipeline

```bash
kubectl create -f .tekton/pipelinerun.yaml
```

## Configuration

### Parameters

- `test-image`: Container image with Strimzi system tests (default: `quay.io/kornys/strimzi-systemtest:latest`)
- `kubeconfig-secret`: Secret containing kubeconfig (default: `kubeconfig`)
- `test-profile`: Maven profile to execute (default: `smoke`)
- `test-groups`: Specific test groups to run (optional, comma-separated)
- `parallel-tests`: Number of parallel test executions (default: `1`)
- `maven-args`: Additional Maven arguments (optional)
- `cleanup-namespace`: Whether to cleanup test namespace after completion (default: `true`)
- `env-configmap`: ConfigMap containing environment variables (optional)

### Available Test Profiles

- `smoke`: Basic smoke tests
- `acceptance`: Acceptance test suite  
- `regression`: Full regression tests
- `upgrade`: Upgrade/downgrade tests
- `performance`: Performance tests
- `all`: All available tests

### Workspaces

The pipeline uses one workspace:

- `test-results`: Persistent storage for test results and artifacts (5Gi PVC created automatically)

### Secrets

The pipeline requires a kubeconfig secret for cluster access, specified by the `kubeconfig-secret` parameter.

Test results include:
- Maven Failsafe/Surefire XML reports
- Test execution logs  
- JSON artifacts

## Usage Examples

### Basic smoke test run

```bash
kubectl create -f - <<EOF
apiVersion: tekton.dev/v1beta1
kind: PipelineRun
metadata:
  name: strimzi-smoke-test-$(date +%s)
spec:
  pipelineRef:
    name: strimzi-systemtest
  params:
    - name: test-profile
      value: "smoke"
    - name: kubeconfig-secret
      value: "kubeconfig"
  workspaces:
    - name: test-results
      volumeClaimTemplate:
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 5Gi
EOF
```

### Run with custom environment variables

```bash
kubectl create -f - <<EOF
apiVersion: tekton.dev/v1beta1
kind: PipelineRun
metadata:
  name: strimzi-custom-env-test-$(date +%s)
spec:
  pipelineRef:
    name: strimzi-systemtest
  params:
    - name: test-profile
      value: "acceptance"
    - name: env-configmap
      value: "strimzi-test-env"
    - name: kubeconfig-secret
      value: "kubeconfig"
  workspaces:
    - name: test-results
      volumeClaimTemplate:
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 5Gi
EOF
```

### Run specific test groups

```bash
kubectl create -f - <<EOF
apiVersion: tekton.dev/v1beta1
kind: PipelineRun
metadata:
  name: strimzi-kafka-security-test-$(date +%s)
spec:
  pipelineRef:
    name: strimzi-systemtest
  params:
    - name: test-groups
      value: "kafka,security"
    - name: parallel-tests
      value: "2"
    - name: kubeconfig-secret
      value: "kubeconfig"
  workspaces:
    - name: test-results
      volumeClaimTemplate:
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 5Gi
EOF
```

## Monitoring

### Check pipeline status

```bash
# List all pipeline runs
kubectl get pipelinerun

# View logs in real-time (requires tkn CLI)
tkn pipelinerun logs <pipelinerun-name> -f

# Get detailed status
kubectl describe pipelinerun <pipelinerun-name>

# Check task status
kubectl get taskrun
```

### Monitor test progress

```bash
# Watch pipeline run status
kubectl get pipelinerun -w

# Check test namespace resources
kubectl get all -n <test-namespace>

# View pod logs directly
kubectl logs -n <test-namespace> <pod-name> -f
```

## Accessing Test Results

Test results are stored in the `test-results` workspace PVC.

### View test summary

```bash
# List all PVCs to find the test results PVC
kubectl get pvc

# Copy the PVC name and create a simple viewer pod
PVC_NAME="<copy-pvc-name-here>"

# Create pod using a simple yaml approach
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: test-results-viewer
spec:
  containers:
  - name: viewer
    image: registry.access.redhat.com/ubi9/ubi-minimal:latest
    command: ["sleep", "3600"]
    volumeMounts:
    - name: test-results
      mountPath: /test-results
  volumes:
  - name: test-results
    persistentVolumeClaim:
      claimName: $PVC_NAME
  restartPolicy: Never
EOF

# Wait for pod to be ready
kubectl wait --for=condition=Ready pod/test-results-viewer --timeout=60s

# Access the pod
kubectl exec -it test-results-viewer -- bash

# Inside the pod, view results
ls -la /test-results/
cat /test-results/test-summary.txt

# Clean up when done
kubectl delete pod test-results-viewer
```

### Copy results locally

```bash
# Copy all test results to local directory
kubectl cp test-results-viewer:/test-results ./local-test-results

# Or copy specific files
kubectl cp test-results-viewer:/test-results/test-summary.txt ./test-summary.txt
```

## Environment Variables Configuration

The pipeline supports loading environment variables from a ConfigMap. The ConfigMap must contain an `env-vars` key with environment variable definitions.

### ConfigMap format

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-test-env
data:
  env-vars: |
    # Format: VARIABLE_NAME=value
    KAFKA_TIERED_STORAGE_BASE_IMAGE=quay.io/strimzi/kafka:latest-kafka-4.0.0
    CLUSTER_OPERATOR_INSTALL_TYPE=Yaml
    BRIDGE_IMAGE=latest-released
    STRIMZI_LOG_LEVEL=DEBUG
```

### Using environment variables

1. Create your ConfigMap:
```bash
kubectl apply -f my-env-configmap.yaml
```

2. Reference it in your PipelineRun:
```yaml
params:
  - name: env-configmap
    value: "my-test-env"
```

The environment variables will be automatically loaded and available to all test steps.

## Cleanup

```bash
# Delete completed pipeline runs
kubectl delete pipelinerun --field-selector='status.conditions[0].status=True'

# Delete test namespace (if cleanup was disabled)
kubectl delete namespace <test-namespace>

# Delete test results PVCs (optional - contains test artifacts)
kubectl delete pvc -l app.kubernetes.io/component=strimzi-systemtest

# Clean up temporary pods
kubectl delete pod test-results-viewer
```

## Troubleshooting

### Common issues

1. **Pipeline stuck in pending**: Check if PVC can be created and bound
2. **Kubeconfig access denied**: Verify secret contains valid kubeconfig
3. **Test image pull failures**: Check container registry access and image name
4. **Environment variables not loaded**: Ensure ConfigMap exists and has `env-vars` key

### Debug commands

```bash
# Check pipeline run events
kubectl describe pipelinerun <name>

# Check task run logs
kubectl logs <taskrun-pod-name>

# Verify kubeconfig secret
kubectl get secret kubeconfig -o yaml

# Check ConfigMap content
kubectl get configmap <env-configmap-name> -o yaml
```