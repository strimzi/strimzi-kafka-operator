/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.crdgenerator.annotations.KubeLink;

public class OpenShiftOriginLinker implements Linker {

    private final String baseUrl;

    public OpenShiftOriginLinker(String baseUrl) {
        //https://docs.openshift.org/3.9/rest_api/
        this.baseUrl = baseUrl;
    }
    @Override
    public String link(KubeLink kubeLink) {
        // https://docs.openshift.org/3.9/rest_api/apis-extensions/v1beta1.DaemonSet.html
        return baseUrl + "apis-" + kubeLink.group() + "/" + kubeLink.version() + "." + kubeLink.kind() + ".html";
    }
}
