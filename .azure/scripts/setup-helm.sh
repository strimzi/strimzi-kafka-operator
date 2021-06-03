#!/usr/bin/env bash
set -x

function install_nsenter {
    # Pre-req for helm
    curl https://repo.phenix.carrefour.com/common/util-linux/util-linux-${TEST_NSENTER_VERSION}.tar.gz  -k --output util-linux-${TEST_NSENTER_VERSION}.tar.gz
    tar -xzf util-linux-${TEST_NSENTER_VERSION}.tar.gz
    cd util-linux-${TEST_NSENTER_VERSION}
    ./configure --without-ncurses
    make nsenter
    sudo cp nsenter /usr/bin
}

function install_helm3 {
    install_nsenter

    export HELM_INSTALL_DIR=/usr/bin
    curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get > get_helm.sh
    # we need to modify the script with a different path because on the Azure pipelines the HELM_INSTALL_DIR env var is not honoured
    sed -i 's#/usr/local/bin#/usr/bin#g' get_helm.sh
    sed -i 's~https://get.helm.sh~https://repo.phenix.carrefour.com/common/helm~g' get_helm.sh
    sed -i 's/curl -SsL/curl -kSsL/g' get_helm.sh
    sed -i 's/"$sum" != "$expected_sum"/1 == 0/g' get_helm.sh
    chmod 700 get_helm.sh

    echo "Installing helm 3..."
    sudo ./get_helm.sh --version "${TEST_HELM3_VERSION}"

    echo "Verifying the installation of helm binary..."
    # run a proper helm command instead of, for example, "which helm", to verify that we can call the binary
    helm --help
    helmCommandOutput=$?

    if [ $helmCommandOutput != 0 ]; then
        echo "helm binary hasn't been installed properly - exiting..."
        exit 1
    fi
}

install_helm3
