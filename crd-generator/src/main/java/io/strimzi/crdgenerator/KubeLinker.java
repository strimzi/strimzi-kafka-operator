/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.crdgenerator.annotations.KubeLink;

public class KubeLinker implements Linker {
    private final String baseUrl;

    public KubeLinker(String baseUrl) {
        //https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/
        this.baseUrl = baseUrl;
    }

    @Override
    public String link(KubeLink kubeLink) {
        //https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#networkpolicyingressrule-v1-networking-k8s-io
        return baseUrl + "#" + kubeLink.kind() + "-" + kubeLink.version() + "-" + kubeLink.group().replace(".", "-");
    }
}
