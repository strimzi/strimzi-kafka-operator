# Custom resource monitoring

## Usage

For deploying the monitoring of custom resources (CR) the [kube-state-metrics Helm chart](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-state-metrics) is used.
This assumes that [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator) is used.

```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install [RELEASE_NAME] prometheus-community/kube-state-metrics -f kube-state-metrics-values.yaml
```
