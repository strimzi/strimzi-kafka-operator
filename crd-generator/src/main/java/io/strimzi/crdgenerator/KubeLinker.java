/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.crdgenerator.annotations.KubeLink;

public class KubeLinker implements Linker {
    private final String baseUrl;

    public KubeLinker(String baseUrl) {
        //https://v1-9.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/
        this.baseUrl = baseUrl;
    }

    @Override
    public String link(KubeLink kubeLink) {
        //https://v1-9.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#ingress-v1beta1-extensions
        return baseUrl + "#" + kubeLink.kind() + "-" + kubeLink.version() + "-" + kubeLink.group();
    }
}
