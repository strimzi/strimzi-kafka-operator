#!/usr/bin/env bash
set -xe

rm -rf ~/.kube

KUBE_VERSION=${KUBE_VERSION:-1.21.0}
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

    curl -Lo kind ${TEST_KUBERNETES_URL} && chmod +x kind
    sudo cp kind /usr/local/bin
}

function create_cluster_role_binding_admin {
    kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=kube-system:default
}

: '
@brief: Set up Kubernetes configuration directory and file.
@note: Ensures $HOME/.kube directory and $HOME/.kube/config file exist.
'
function setup_kube_directory {
    mkdir $HOME/.kube || true
    touch $HOME/.kube/config
}

: '
@brief: Add Docker Hub credentials to Kubernetes node.
@param $1: Container name/ID.
@global: COPY_DOCKER_LOGIN - If "true", copies credentials.
@note: Uses hosts $HOME/.docker/config.json.
'
function add_docker_hub_credentials_to_kubernetes {
    # Add Docker hub credentials to Minikube
    if [ "$COPY_DOCKER_LOGIN" = "true" ]
    then
      set +ex

      docker exec $1 bash -c "echo '$(cat $HOME/.docker/config.json)'| sudo tee -a /var/lib/kubelet/config.json > /dev/null && sudo systemctl restart kubelet"

      set -ex
    fi
}

: '
@brief: Update Docker daemon configuration and restart service.
@param $1: JSON string for Docker daemon configuration.
@note: Requires sudo permissions.
'
function updateDockerDaemonConfiguration() {
    # We need to add such host to insecure-registry (as localhost is default)
    echo $1 | sudo tee /etc/docker/daemon.json
    # we need to restart docker service to propagate configuration
    systemctl restart docker
}

setup_kube_directory
install_kubectl
install_kubernetes_provisioner

reg_name='kind-registry'
reg_port='5001'

if [[ "$IP_FAMILY" = "ipv4" || "$IP_FAMILY" = "dual" ]]; then
    hostname=$(hostname --ip-address | grep -oE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' | awk '$1 != "127.0.0.1" { print $1 }' | head -1)

    # update insecure registries
    updateDockerDaemonConfiguration "{ \"insecure-registries\" : [\"${hostname}:${reg_port}\"] }"

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
    # run local container registry
    if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        docker run \
          -d --restart=always -p "${hostname}:${reg_port}:5000" --name "${reg_name}" \
          registry:2
    fi

    REGISTRY_DIR="/etc/containerd/certs.d/${hostname}:${reg_port}"

    # Add the registry config to the nodes
    #
    # This is necessary because localhost resolves to loopback addresses that are
    # network-namespace local.
    # In other words: localhost in the container is not localhost on the host.
    #
    # We want a consistent name that works from both ends, so we tell containerd to
    # alias localhost:${reg_port} to the registry container when pulling images
    # note: kind get nodes (default name `kind` and with specifying new name we have to use --name <cluster-name>
    for node in $(kind get nodes --name kind-cluster); do
        echo "Executing command in node:${node}"
        docker exec "${node}" mkdir -p "${REGISTRY_DIR}"
        cat <<EOF | docker exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
    [host."http://${reg_name}:5000"]
EOF
    done
elif [[ "$IP_FAMILY" = "ipv6" ]]; then
    # for ipv6 configuration
    ula_fixed_ipv6="fd01:2345:6789"
    registry_dns="myregistry.local"

    # manually assign an IPv6 address to eth0 interface
    sudo ip -6 addr add "${ula_fixed_ipv6}"::1/64 dev eth0

     # use ULA (i.e., Unique Local Address), which offers a similar "private" scope as link-local
    # but without the interface dependency and some of the other challenges of link-local addresses.
    # (link-local starts as fe80::) but we will use ULA fd01
    updateDockerDaemonConfiguration "{
        \"insecure-registries\" : [\"[${ula_fixed_ipv6}::1]:${reg_port}\", \"${registry_dns}:${reg_port}\"],
        \"experimental\": true,
        \"ip6tables\": true,
        \"fixed-cidr-v6\": \"${ula_fixed_ipv6}::/80\"
    }"

    cat <<EOF | kind create cluster --config=-
    kind: Cluster
    apiVersion: kind.x-k8s.io/v1alpha4
    name: kind-cluster
    containerdConfigPatches:
    - |-
      [plugins."io.containerd.grpc.v1.cri".registry.mirrors."myregistry.local:5001"]
          endpoint = ["http://myregistry.local:5001"]
    networking:
        ipFamily: $IP_FAMILY
EOF
    # run local container registry
    if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        docker run \
          -d --restart=always -p "[${ula_fixed_ipv6}::1]:${reg_port}:5000" --name "${reg_name}" \
          registry:2
    fi
    # we need to also make a DNS record for docker tag because it seems that such version does not support []:: format
    echo "${ula_fixed_ipv6}::1    ${registry_dns}" >> /etc/hosts

    # note: kind get nodes (default name `kind` and with specifying new name we have to use --name <cluster-name>
    for node in $(kind get nodes --name kind-cluster); do
        echo "Executing command in node:${node}"
        # add myregistry.local to each node to resolve our IPv6 address
        docker exec "${node}" /bin/sh -c "echo \"${ula_fixed_ipv6}::1    ${registry_dns}\" >> /etc/hosts"
    done
fi

# Connect the registry to the cluster network if not already connected
# This allows kind to bootstrap the network but ensures they're on the same network
if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = 'null' ]; then
  docker network connect "kind" "${reg_name}"
fi

add_docker_hub_credentials_to_kubernetes "kind"

create_cluster_role_binding_admin
label_node