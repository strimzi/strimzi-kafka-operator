/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import java.nio.file.Path;
import java.util.Map;

public interface HelmClient {
    static HelmClient findClient(KubeCmdClient<?> kubeClient) {
        HelmClient client = new Helm(kubeClient);
        if (!client.clientAvailable()) {
            throw new RuntimeException("No helm client found on $PATH. $PATH=" + System.getenv("PATH"));
        }
        return client;
    }

    /** Initialize the Helm Tiller server on the cluster */
    HelmClient init();

    /** Install a chart given its local path, release name, and values to override */
    HelmClient install(Path chart, String releaseName, Map<String, String> valueOverrides);

    /** Delete a chart given its release name */
    HelmClient delete(String releaseName);

    boolean clientAvailable();
}
