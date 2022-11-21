/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.crdgenerator.annotations.KubeLink;

/**
 * CLass for handling links to Kubernetes documentation
 */
public class KubeLinker implements Linker {
    private final String baseUrl;

    /**
     * Constructs the KubeLinker and initializes the base URL to the Kubernetes documentation
     *
     * @param baseUrl   The base URL to the Kubernetes documentation which should be used
     */
    public KubeLinker(String baseUrl) {
        // E.g. https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/
        this.baseUrl = baseUrl;
    }

    /**
     * Generates an URL to the Kubernetes documentation to some specific part of the Kube API
     *
     * @param kubeLink  Specifies the Kubernetes API which should the link point to
     *
     * @return  An HTTP link deep-linking to the Kubernetes documentation
     */
    @Override
    public String link(KubeLink kubeLink) {
        // E.g. https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#networkpolicyingressrule-v1-networking-k8s-io
        return baseUrl + "#" + kubeLink.kind() + "-" + kubeLink.version() + "-" + kubeLink.group().replace(".", "-");
    }
}
