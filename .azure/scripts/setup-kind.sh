#!/usr/bin/env bash
set -xe
set -o errexit

rm -rf ~/.kube

KUBE_VERSION=${KUBE_VERSION:-1.21.0}
MINIKUBE_REGISTRY_IMAGE=${REGISTRY_IMAGE:-"registry"}
COPY_DOCKER_LOGIN=${COPY_DOCKER_LOGIN:-"false"}

DEFAULT_CLUSTER_MEMORY=$(free -m | grep "Mem" | awk '{print $2}')
DEFAULT_CLUSTER_CPU=$(awk '$1~/cpu[0-9]/{usage=($2+$4)*100/($2+$4+$5); print $1": "usage"%"}' /proc/stat | wc -l)

CLUSTER_MEMORY=${CLUSTER_MEMORY:-$DEFAULT_CLUSTER_MEMORY}
CLUSTER_CPU=${CLUSTER_CPU:-$DEFAULT_CLUSTER_CPU}

echo "[INFO] CLUSTER_MEMORY: ${CLUSTER_MEMORY}"
echo "[INFO] CLUSTER_CPU: ${CLUSTER_CPU}"

# note that IPv6 is only supported on kind (i.e., minikube does not support it). Also we assume that when you set this flag
# to true then you meet requirements (i.) net.ipv6.conf.all.disable_ipv6 = 0 (ii. you have installed CNI supporting IPv6)
IP_FAMILY=${IP_FAMILY:-"ipv4"}

ARCH=$1
if [ -z "$ARCH" ]; then
    ARCH="amd64"
fi

function install_kubectl {
    if [ "${TEST_KUBECTL_VERSION:-latest}" = "latest" ]; then
        TEST_KUBECTL_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    fi
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${TEST_KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl && chmod +x kubectl
    sudo cp kubectl /usr/local/bin
}

function label_node {
	# It should work for all clusters
	for nodeName in $(kubectl get nodes -o custom-columns=:.metadata.name --no-headers);
	do
		echo ${nodeName};
		kubectl label node ${nodeName} rack-key=zone;
	done
}


function install_kubernetes_provisioner {

    if [ "${TEST_KUBERNETES_VERSION:-latest}" = "latest" ]; then
        # get the latest released tag
        TEST_KUBERNETES_VERSION=$(curl https://api.github.com/repos/kubernetes-sigs/kind/releases/latest | grep -Po "(?<=\"tag_name\": \").*(?=\")")
    fi
    TEST_KUBERNETES_URL=https://github.com/kubernetes-sigs/kind/releases/download/${TEST_KUBERNETES_VERSION}/kind-linux-${ARCH}

    if [ "$KUBE_VERSION" != "latest" ] && [ "$KUBE_VERSION" != "stable" ]; then
        KUBE_VERSION="v${KUBE_VERSION}"
    fi

    curl -Lo $TEST_CLUSTER ${TEST_KUBERNETES_URL} && chmod +x $TEST_CLUSTER
    sudo cp $TEST_CLUSTER /usr/local/bin
}

function create_cluster_role_binding_admin {
    kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=kube-system:default
}

function setup_kube_directory {
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config
}

function add_docker_hub_credentials_to_kubernetes {
    # Add Docker hub credentials to Minikube
    if [ "$COPY_DOCKER_LOGIN" = "true" ]
    then
      set +ex

      docker exec $1 bash -c "echo '$(cat $HOME/.docker/config.json)'| sudo tee -a /var/lib/kubelet/config.json > /dev/null && sudo systemctl restart kubelet"

      set -ex
    fi
}

setup_kube_directory
install_kubectl
install_kubernetes_provisioner

reg_name='kind-registry'
reg_port='5001'
hostname=''
if [ "$IP_FAMILY" = "ipv4" ]; then
    hostname=$(hostname --ip-address | grep -oE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' | awk '$1 != "127.0.0.1" { print $1 }' | head -1)
else
    # for ipv6 and dual configuration
    hostname=$(ip -6 addr show eth0 | awk '/inet6/ {print $2}' | cut -d '/' -f 1)
fi

 daemon_configuration=''

if [ "$IP_FAMILY" = "ipv6" ]; then
  daemon_configuration="{
          \"insecure-registries\" : [\"${hostname}:${reg_port}\"],
          \"experimental\": true,
          \"ip6tables\": true
       }"
else
  daemon_configuration="{
          \"insecure-registries\" : [\"${hostname}:${reg_port}\"]
       }"
fi

# We need to add such host to insecure-registry (as localhost is default)
echo ${daemon_configuration} | sudo tee /etc/docker/daemon.json
# we need to restart docker service to propagate configuration
systemctl restart docker

# Create kind cluster with containerd registry config dir enabled
# TODO: kind will eventually enable this by default and this patch will
# be unnecessary.
#
# See:
# https://github.com/kubernetes-sigs/kind/issues/2875
# https://github.com/containerd/containerd/blob/main/docs/cri/config.md#registry-configuration
# See: https://github.com/containerd/containerd/blob/main/docs/hosts.md
cat <<EOF | kind create cluster --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: kind-cluster
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry]
    config_path = "/etc/containerd/certs.d"
networking:
    ipFamily: $IP_FAMILY
EOF

if [ "$IP_FAMILY" = "ipv4" ]; then
    if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        docker run \
          -d --restart=always -p "${hostname}:${reg_port}:5000" --name "${reg_name}" \
          registry:2
    fi
else
    # for ipv6 and dual configuration
    if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        docker run \
          -d --restart=always --ip6 ${hostname} -p "${reg_port}:5000" --name "${reg_name}" \
          registry:2
    fi
fi

# Add the registry config to the nodes
#
# This is necessary because localhost resolves to loopback addresses that are
# network-namespace local.
# In other words: localhost in the container is not localhost on the host.
#
# We want a consistent name that works from both ends, so we tell containerd to
# alias localhost:${reg_port} to the registry container when pulling images
REGISTRY_DIR="/etc/containerd/certs.d/${hostname}:${reg_port}"
# note: kind get nodes (default name `kind` and with specifying new name we have to use --name <cluster-name>
for node in $(kind get nodes --name kind-cluster); do
  echo "Executing command in node:${node}"
  docker exec "${node}" mkdir -p "${REGISTRY_DIR}"
  cat <<EOF | docker exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
[host."http://${reg_name}:5000"]
EOF
done

# Connect the registry to the cluster network if not already connected
# This allows kind to bootstrap the network but ensures they're on the same network
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = 'null' ]; then
  docker network connect "kind" "${reg_name}"
fi

# Document the local registry
# https://github.com/kubernetes/enhancements/tree/master/keps/sig-cluster-lifecycle/generic/1755-communicating-a-local-registry
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "${hostname}:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

add_docker_hub_credentials_to_kubernetes "$TEST_CLUSTER"

create_cluster_role_binding_admin
label_node