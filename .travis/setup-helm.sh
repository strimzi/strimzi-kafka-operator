#!/usr/bin/env bash
set -x

function install_helm2 {
    install_nsenter

    export HELM_INSTALL_DIR=/usr/bin
    curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get > get_helm.sh
    # we need to modify the script with a different path because on the Azure pipelines the HELM_INSTALL_DIR env var is not honoured
    sed -i 's#/usr/local/bin#/usr/bin#g' get_helm.sh
    chmod 700 get_helm.sh

    echo "Installing helm 2..."
    sudo ./get_helm.sh --version "${TEST_HELM2_VERSION}"
    sudo mv ${HELM_INSTALL_DIR}/helm ${HELM_INSTALL_DIR}/helm2

    echo "Verifying the installation of helm2 binary..."
    # run a proper helm command instead of, for example, "which helm", to verify that we can call the binary
    helm2 --help
    helmCommandOutput=$?

    if [ $helmCommandOutput != 0 ]; then
        echo "helm2 binary hasn't been installed properly - exiting..."
        exit 1
    fi
}

function install_helm3 {
    install_nsenter

    export HELM_INSTALL_DIR=/usr/bin
    curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get > get_helm.sh
    # we need to modify the script with a different path because on the Azure pipelines the HELM_INSTALL_DIR env var is not honoured
    sed -i 's#/usr/local/bin#/usr/bin#g' get_helm.sh
    chmod 700 get_helm.sh

    echo "Installing helm 3..."
    sudo ./get_helm.sh --version "${TEST_HELM3_VERSION}"
    sudo mv ${HELM_INSTALL_DIR}/helm ${HELM_INSTALL_DIR}/helm3

    echo "Verifying the installation of helm3 binary..."
    # run a proper helm command instead of, for example, "which helm", to verify that we can call the binary
    helm3 --help
    helmCommandOutput=$?

    if [ $helmCommandOutput != 0 ]; then
        echo "helm3 binary hasn't been installed properly - exiting..."
        exit 1
    fi
}

install_helm2
install_helm3
