/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.kubernetes;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceType;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ConfigMapResource implements ResourceType<ConfigMap> {

    @Override
    public String getKind() {
        return Constants.CONFIG_MAP;
    }
    @Override
    public ConfigMap get(String namespace, String name) {
        return kubeClient(namespace).getConfigMap(namespace, name);
    }
    @Override
    public void create(ConfigMap resource) {
        kubeClient().createOrReplaceConfigMap(resource);
    }
    @Override
    public void delete(ConfigMap resource) {
        kubeClient().deleteConfigMap(resource);
    }
    @Override
    public boolean waitForReadiness(ConfigMap resource) {
        return resource != null;
    }
}
