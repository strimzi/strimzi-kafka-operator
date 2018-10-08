/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * This class is a temporary work-around for the fact that Fabric8 doesn't
 * yet support an API for manipulating Kubernetes ClusterRoles
 * @deprecated This can be removed once support for ClusterRoles and ClusterRoleBindings is in Fabric8.
 */
@Deprecated
public class ClusterRoleOperator extends WorkaroundRbacOperator<ClusterRoleOperator.ClusterRole> {

    public ClusterRoleOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "rbac.authorization.k8s.io", "v1beta1", "clusterroles");
    }

    public static class ClusterRole {
        private final String resource;

        public ClusterRole(String yaml) {
            this.resource = convertYamlToJson(yaml);
        }

        private String convertYamlToJson(String yaml) {
            try {
                ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
                Object obj = yamlReader.readValue(yaml, Object.class);
                ObjectMapper jsonWriter = new ObjectMapper();
                return jsonWriter.writeValueAsString(obj);
            } catch (IOException e)   {
                throw new RuntimeException(e);
            }
        }

        public String toString() {
            return resource;
        }
    }

    private String urlWithoutName() {
        return baseUrl + "apis/" + group + "/" + apiVersion + "/" + plural;
    }

    private String urlWithName(String name) {
        return urlWithoutName() + "/" + name;
    }

    public Future<Void> reconcile(String name, ClusterRole resource) {
        return doReconcile(urlWithoutName(), urlWithName(name), resource);
    }

}
