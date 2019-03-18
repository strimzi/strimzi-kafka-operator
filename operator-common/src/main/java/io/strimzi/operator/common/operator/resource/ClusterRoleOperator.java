/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRole;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRole;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import java.io.IOException;

public class ClusterRoleOperator extends AbstractNonNamespacedResourceOperator<KubernetesClient,
        KubernetesClusterRole, KubernetesClusterRoleList, DoneableKubernetesClusterRole, Resource<KubernetesClusterRole,
        DoneableKubernetesClusterRole>> {

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */

    public ClusterRoleOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "ClusterRole");
    }

    @Override
    protected MixedOperation<KubernetesClusterRole, KubernetesClusterRoleList, DoneableKubernetesClusterRole,
            Resource<KubernetesClusterRole, DoneableKubernetesClusterRole>> operation() {
        return client.rbac().kubernetesClusterRoles();
    }

    public static KubernetesClusterRole convertYamlToClusterRole(String yaml) {
        try {
            ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
            KubernetesClusterRole cr = yamlReader.readValue(yaml, KubernetesClusterRole.class);
            return cr;
        } catch (IOException e)   {
            throw new RuntimeException(e);
        }
    }
}
