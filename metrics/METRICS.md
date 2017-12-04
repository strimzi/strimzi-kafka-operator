This section describes how it's possible to deploy a Prometheus server for scraping metrics from the Kafka cluster and showing them using a Grafana dashboard. The provided resources for deploying can't be considered production grade; they are used just as an example to show how it's possible to use the metrics feature provided in this project.

# Deploying on OpenShift

## Prometheus

The Prometheus server configuration uses a service discovery feature in order to discover the pods in the cluster from which it gets metrics.
In order to have this feature working, it's necessary to provide access to the API server to get the pod list to the service account used for running the Prometheus service pod (the service account `prometheus-server` is used).

```
export NAMESPACE=[namespace]
oc login -u system:admin
oc create sa prometheus-server
oc adm policy add-cluster-role-to-user cluster-reader system:serviceaccount:${NAMESPACE}:prometheus-server
oc login -u developer
```

where `[namespace]` is the namespace/project where the Apache Kafka cluster was deployed.

After that, it's possible to deploy the Prometheus server in the cluster :

1. Create the [provided "prometheus" template](prometheus/openshift-template.yaml) by running

        oc create -f metrics/prometheus/openshift-template.yaml

2. Create a new app using the "prometheus" template:

        oc new-app prometheus

## Grafana

A Grafana server is necessary only to get a UI for the Prometheus metrics. You can use the following [project](https://github.com/OpenShiftDemos/grafana-openshift) which you need to clone locally.

From the `grafana-openshift` directory, run

```
chmod +x root/usr/bin/fix-permissions 
chmod +x run.sh 
```

to set "execute" permissions on the above scripts and then run

```
oc new-build --binary --name=grafana
oc start-build grafana --from-dir=. --follow
```

to start the image building process inside the OpenShift cluster. 
After a while, the grafana image will be available in the internal Docker registry of the OpenShift cluster and the last step is just creating a new application with the provided image.

```
oc new-app grafana
```

# Deploying on Kubernetes

## Prometheus


The Prometheus server configuration uses a service discovery feature in order to discover the pods in the cluster from which it gets metrics.
In order to have this feature working, it's necessary to provide access to the API server to get the pod list to the service account used for running the Prometheus service pod (the service account `prometheus-server` is used) if the RBAC is enabled in your Kubernetes deployment.

```
export NAMESPACE=[namespace]
kubectl create sa prometheus-server
kubectl create -f metrics/prometheus/cluster-reader.yaml
kubectl create clusterrolebinding read-pods-binding --clusterrole=cluster-reader --serviceaccount=${NAMESPACE}:prometheus-server
```

where `[namespace]` is the namespace/project where the Apache Kafka cluster was deployed.

Finally, create the Prometheus service by running

        kubectl apply -f metrics/prometheus/kubernetes.yaml

## Grafana

A Grafana server is necessary only to get a UI for the Prometheus metrics. You can use the following [project](https://github.com/OpenShiftDemos/grafana-openshift) which you need to clone locally.

From the `grafana-openshift` directory, run

```
chmod +x root/usr/bin/fix-permissions 
chmod +x run.sh 
```

to set "execute" permissions on the above needed scripts.
If you are using `minikube` for running the Kubernetes cluster then you have to build the Grafana image inside the cluster itself in order to have it available in the internal Docker registry. To use the Docker environment in `minikube` run

```
eval $(minikube docker-env)
```

After that it's possible to build the image by running

```
docker build -t enmasseproject/grafana:latest .
```

Finally you can execute the actual deployment by running

```
kubectl apply -f metrics/grafana/kubernetes.yaml
```
