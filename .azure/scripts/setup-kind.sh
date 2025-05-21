#!/usr/bin/env bash
set -xe

rm -rf ~/.kube

# There is a bug in 0.24.0 - https://github.com/kubernetes-sigs/kind/issues/3713
KIND_VERSION=${KIND_VERSION:-"v0.28.0"}
KIND_CLOUD_PROVIDER_VERSION=${KIND_CLOUD_PROVIDER_VERSION:-"v0.6.0"}
# To properly upgrade Kind version check the releases in github https://github.com/kubernetes-sigs/kind/releases and use proper image based on Kind version
KIND_NODE_IMAGE=${KIND_NODE_IMAGE:-"kindest/node:v1.25.16@sha256:5da57dfc290ac3599e775e63b8b6c49c0c85d3fec771cd7d55b45fae14b38d3b"}
COPY_DOCKER_LOGIN=${COPY_DOCKER_LOGIN:-"false"}
DOCKER_CMD="${DOCKER_CMD:-docker}"
REGISTRY_IMAGE=${REGISTRY_IMAGE:-"registry:2"}
CONTROL_NODES=${CONTROL_NODES:-3}
WORKER_NODES=${WORKER_NODES:-3}

KIND_CLUSTER_NAME="kind-cluster"

# note that IPv6 is only supported on kind (i.e., minikube does not support it). Also we assume that when you set this flag
# to true then you meet requirements (i.) net.ipv6.conf.all.disable_ipv6 = 0 (ii. you have installed CNI supporting IPv6)
IP_FAMILY=${IP_FAMILY:-"ipv4"}

ARCH=$1
if [ -z "$ARCH" ]; then
    ARCH="amd64"
fi

function is_docker() {
    [[ "$DOCKER_CMD" == "docker" ]]
}

function is_podman() {
    [[ "$DOCKER_CMD" == "podman" ]]
}

function install_kubectl {
    if [ "${TEST_KUBECTL_VERSION:-latest}" = "latest" ]; then
        TEST_KUBECTL_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)
    fi
    curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${TEST_KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl && chmod +x kubectl
    sudo cp kubectl /usr/local/bin

    if is_podman; then
        sudo ln -sf /usr/local/bin/kubectl /usr/bin/kubectl
    fi
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

    KIND_URL=https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-${ARCH}

    curl -Lo kind ${KIND_URL} && chmod +x kind

    # Move the binary to a globally accessible location
    sudo mv kind /usr/local/bin/kind
    sudo ln -sf /usr/local/bin/kind /usr/bin/kind

    if command -v kind >/dev/null 2>&1; then
        echo "Kind installed successfully at $(command -v kind)"
        kind version
    else
        echo "Error: Kind installation failed."
        exit 1
    fi
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
@brief: Fixes the "ip6_tables/ip_tables" issue for Podman.
@note: Ensures the required kernel modules are loaded. Fixes:
Command Output: Error: netavark: code: 3, msg: modprobe: ERROR: could not insert 'ip_tables': Operation not permitted
iptables v1.8.10 (legacy): Table does not exist (do you need to insmod?)
'
function load_iptables_modules_for_podman {
  if is_podman; then
    echo "Ensuring ip_tables and ip6_tables modules are loaded..."
    for module in ip_tables ip6_tables; do
      if ! lsmod | grep -q "$module"; then
        sudo modprobe "$module" || {
          echo "Error: Failed to load $module. Ensure your kernel supports it."
          exit 1
        }
      fi
    done
  fi
}

: '
@brief: Updates container runtime (Docker/Podman) configurations to allow insecure local registries.
@param:
        1) registry_address - IPv4 registry address, e.g., "192.168.0.2:5000"
        2) ula_fixed_ipv6 - Unique local address prefix for IPv6-based setups
        3) reg_port - Port where the registry is running
        4) registry_dns - DNS name for registry
@global:
        IP_FAMILY - Determines whether to configure IPv4 or IPv6 settings
@note: Adjusts the container runtime’s config files (/etc/docker/daemon.json
       or /etc/containers/registries.conf) and restarts the respective service.
'
function updateContainerRuntimeConfiguration {
    local registry_address="$1"
    local ula_fixed_ipv6="$2"
    local reg_port="$3"
    local registry_dns="$4"

    local docker_config podman_config

    # Build configurations depending on IP family
    if [[ "$IP_FAMILY" == "ipv6" ]]; then
        docker_config=$(cat <<EOF
{
  "insecure-registries": ["[${ula_fixed_ipv6}::1]:${reg_port}", "${registry_dns}:${reg_port}"],
  "experimental": true,
  "ip6tables": true,
  "fixed-cidr-v6": "${ula_fixed_ipv6}::/80"
}
EOF
)
        podman_config=$(cat <<EOF
[registries.insecure]
registries = ["[${ula_fixed_ipv6}::1]:${reg_port}", "${registry_dns}:${reg_port}"]
EOF
)
    else
        docker_config=$(cat <<EOF
{
  "insecure-registries": ["${registry_address}"]
}
EOF
)
        podman_config=$(cat <<EOF
[[registry]]
location = "${registry_address}"
insecure = true
EOF
)
    fi

    if is_docker; then
        echo "$docker_config" | tee /etc/docker/daemon.json
        systemctl restart docker
    else
        # Ensure the podman_config is inserted correctly after unqualified-search-registries
        sudo awk -v config="$podman_config" '
        BEGIN { inserted = 0 }
        /unqualified-search-registries/ && !inserted {
            print $0
            print config
            inserted = 1
            next
        }
        { print $0 }
        ' /etc/containers/registries.conf > /tmp/registries.conf

        sudo mv /tmp/registries.conf /etc/containers/registries.conf
        sudo systemctl restart podman
    fi
}

: '
@brief: Increases the inotify user watches and user instances limits on a Linux system.
@param: None.
@global: None.
@note: Inotify is a Linux subsystem used for file system event notifications. This function
       helps adjust the limits for applications or services that monitor a large number
       of files or directories.
       This is specifically needed for multi-node control plane cluster
       https://github.com/kubernetes-sigs/kind/issues/2744#issuecomment-1127808069
'
function adjust_inotify_limits {
    # Increase the inotify user watches limit
    echo "Setting fs.inotify.max_user_watches to 655360..."
    echo fs.inotify.max_user_watches=655360 | sudo tee -a /etc/sysctl.conf

    # Increase the inotify user instances limit
    echo "Setting fs.inotify.max_user_instances to 1280..."
    echo fs.inotify.max_user_instances=1280 | sudo tee -a /etc/sysctl.conf

    # Reload the system configuration settings
    echo "Reloading sysctl settings..."
    sudo sysctl -p

    echo "Inotify limits adjusted successfully."
}

: '
@brief: Creates a KIND cluster configuration and provisions the cluster.
@param:
        1) control_planes - Number of control-plane nodes.
@global:
        KIND_CLUSTER_NAME - Name of the KIND cluster.
        KIND_NODE_IMAGE   - KIND node image to use.
        IP_FAMILY         - Determines whether to configure IPv4 or IPv6 settings
@note: Writes the cluster configuration to a temporary file before creating the cluster.
'
function create_kind_cluster {
    local control_planes="$1"
    local worker_nodes="$2"

# Start the cluster configuration
    cat <<EOF > /tmp/kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
EOF

    # Add control-plane nodes
    for i in $(seq 1 "$control_planes"); do
        echo "    - role: control-plane" >> /tmp/kind-config.yaml
    done

    # Add worker nodes
    for i in $(seq 1 "$worker_nodes"); do
        echo "    - role: worker" >> /tmp/kind-config.yaml
    done

    # Add specific containerd configuration for IPv4/IPv6
    if [[ "$IP_FAMILY" == "ipv6" ]]; then
        cat <<EOF >> /tmp/kind-config.yaml
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors."myregistry.local:5001"]
      endpoint = ["http://myregistry.local:5001"]
EOF
    else
        cat <<EOF >> /tmp/kind-config.yaml
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "/etc/containerd/certs.d"
EOF
    fi

    # Add networking configuration
    cat <<EOF >> /tmp/kind-config.yaml
networking:
  ipFamily: $IP_FAMILY
EOF


    # Create the KIND cluster
    kind create cluster \
        --image "$KIND_NODE_IMAGE" \
        --name "$KIND_CLUSTER_NAME" \
        --config=/tmp/kind-config.yaml

    echo "KIND cluster '${KIND_CLUSTER_NAME}' created successfully with IP family '${IP_FAMILY}'."
}

: '
@brief: Installs and runs cloud-provider-kind in a container.
@global:
        KIND_CLOUD_PROVIDER_VERSION - Specifies the version of cloud-provider-kind to install.
        DOCKER_CMD - Specifies the container runtime (docker or podman).
@note:
        Supports only Docker for now, as Podman support is pending resolution.
        Uses docker.sock for Docker.
        Ensures the container is not already running before starting it.
        Enables LoadBalancer port mapping (--enable-lb-port-mapping=true).

@see:
        Podman support tracking issue:
        https://github.com/kubernetes-sigs/cloud-provider-kind/issues/221
        Enabling LoadBalancer Port Mapping:
        https://github.com/kubernetes-sigs/cloud-provider-kind?tab=readme-ov-file#enabling-load-balancer-port-mapping
'
function run_cloud_provider_kind() {
    local cloud_provider_kind="cloud-provider-kind"
    local socket_path=""
    local kind_network="$1"

    # Check if using Podman and warn the user
    if [[ "$DOCKER_CMD" == "podman" ]]; then
        echo "[WARNING] cloud-provider-kind does not currently support Podman. See: https://github.com/kubernetes-sigs/cloud-provider-kind/issues/221"
    else
        socket_path="/var/run/docker.sock"

        # Ensure the container is not already running
        if $DOCKER_CMD ps --format "{{.Names}}" | grep -q "$cloud_provider_kind"; then
            echo "[INFO] $cloud_provider_kind is already running."
            return 0
        fi

        echo "[INFO] Starting $cloud_provider_kind with $DOCKER_CMD..."

        $DOCKER_CMD run -d --name "$cloud_provider_kind" \
             --network "$kind_network" \
             -v "$socket_path:/var/run/docker.sock" \
             registry.k8s.io/cloud-provider-kind/cloud-controller-manager:"${KIND_CLOUD_PROVIDER_VERSION}"
    fi
}

: '
@brief: Configures the network for Docker or Podman based on IP family.
@param:
        1) network_name - The name of the network to configure.
@global:
        IP_FAMILY   - Determines whether to configure IPv4 or IPv6 settings.
        DOCKER_CMD  - The container runtime command (docker or podman).
@note: Podman does not support IPv6/Dual stack in this installer. Docker allows explicit IPv6 configuration.
'
function configure_network {
    local network_name="$1"

    if is_podman; then
        # Podman IPv6/Dual
        if [[ "$IP_FAMILY" = "ipv6"  ||  "$IP_FAMILY" = "dual"  ]]; then
            echo "[WARNING] IPv6/Dual not supported by this installer within Podman!"
            exit 1
        else
        # Podman IPv4
            if ! $DOCKER_CMD network exists "${network_name}"; then
                $DOCKER_CMD network create "${network_name}"
            fi
        fi
    elif is_docker; then
        # Docker IPv6/Dual
        if [[ "$IP_FAMILY" = "ipv6"  ||  "$IP_FAMILY" = "dual" ]]; then
            if ! $DOCKER_CMD network inspect "${network_name}" >/dev/null 2>&1; then
                $DOCKER_CMD network create \
                  --ipv6 "${network_name}"
            fi
        else
        # Docker IPv4
           if ! $DOCKER_CMD network inspect "${network_name}" >/dev/null 2>&1; then
               $DOCKER_CMD network create "${network_name}"
           fi
       fi
    else
        echo "[WARNING] Unsupported container engine: $DOCKER_CMD!"
        exit 1
    fi
}

setup_kube_directory
install_kubectl
install_kubernetes_provisioner
adjust_inotify_limits
load_iptables_modules_for_podman

reg_name='kind-registry'
reg_port='5001'
network_name="kind"

configure_network "${network_name}"

if [[ "$IP_FAMILY" = "ipv4" || "$IP_FAMILY" = "dual" ]]; then
    hostname=$(hostname --ip-address | grep -oE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' | awk '$1 != "127.0.0.1" { print $1 }' | head -1)

    # update insecure registries
    updateContainerRuntimeConfiguration "${hostname}:${reg_port}"

    # Create kind cluster with containerd registry config dir enabled
    # TODO: kind will eventually enable this by default and this patch will
    # be unnecessary.
    #
    # See:
    # https://github.com/kubernetes-sigs/kind/issues/2875
    # https://github.com/containerd/containerd/blob/main/docs/cri/config.md#registry-configuration
    # See: https://github.com/containerd/containerd/blob/main/docs/hosts.md
    create_kind_cluster ${CONTROL_NODES} ${WORKER_NODES}
    # run local container registry
    if [ "$($DOCKER_CMD inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        $DOCKER_CMD run \
          -d --restart=always -p "${hostname}:${reg_port}:5000" --name "${reg_name}" --network "${network_name}" \
          ${REGISTRY_IMAGE}
    fi

    # Add the registry config to the nodes
    #
    # This is necessary because localhost resolves to loopback addresses that are
    # network-namespace local.
    # In other words: localhost in the container is not localhost on the host.
    #
    # We want a consistent name that works from both ends, so we tell containerd to
    # alias localhost:${reg_port} to the registry container when pulling images
    # note: kind get nodes (default name `kind` and with specifying new name we have to use --name <cluster-name>
    # See https://kind.sigs.k8s.io/docs/user/local-registry/
    REGISTRY_DIR="/etc/containerd/certs.d/${hostname}:${reg_port}"

    for node in $(kind get nodes --name "${KIND_CLUSTER_NAME}"); do
        echo "Executing command in node:${node}"
        $DOCKER_CMD exec "${node}" mkdir -p "${REGISTRY_DIR}"
        cat <<EOF | $DOCKER_CMD exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
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
    updateContainerRuntimeConfiguration "" "${ula_fixed_ipv6}" "${reg_port}" "${registry_dns}"
    create_kind_cluster ${CONTROL_NODES} ${WORKER_NODES}

    # run local container registry
    if [ "$($DOCKER_CMD inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        $DOCKER_CMD run \
          -d --restart=always -p "[${ula_fixed_ipv6}::1]:${reg_port}:5000" --name "${reg_name}" --network "${network_name}" \
          ${REGISTRY_IMAGE}
    fi
    # we need to also make a DNS record for docker tag because it seems that such version does not support []:: format
    echo "${ula_fixed_ipv6}::1    ${registry_dns}" >> /etc/hosts

    # note: kind get nodes (default name `kind` and with specifying new name we have to use --name <cluster-name>
    # See https://kind.sigs.k8s.io/docs/user/local-registry/
    for node in $(kind get nodes --name "${KIND_CLUSTER_NAME}"); do
        echo "Executing command in node:${node}"
        cat <<EOF | $DOCKER_CMD exec -i "${node}" cp /dev/stdin "/etc/hosts"
${ula_fixed_ipv6}::1    ${registry_dns}
EOF
    done
fi

create_cluster_role_binding_admin
label_node
run_cloud_provider_kind ${network_name}