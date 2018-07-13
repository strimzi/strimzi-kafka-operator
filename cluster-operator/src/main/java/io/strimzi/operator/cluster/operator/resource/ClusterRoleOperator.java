/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
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
        private final String name;

        public ClusterRole(String name) {
            this.name = name;
        }
        public String toString() {
            return "{" +
                     "\"apiVersion\":\"rbac.authorization.k8s.io/v1beta1\"," +
                     "\"kind\":\"ClusterRole\"," +
                     "\"metadata\":{" +
                       "\"name\":\"" + name + "\"," +
                       "\"labels\":{" +
                         "\"app\": \"strimzi\"" +
                       "}" +
                     "}," +
                     "\"rules\":[" +
                       "{" +
                         "\"apiGroups\":[\"\"]," +
                         "\"resources\":[" +
                           "\"nodes\"" +
                         "]," +
                         "\"verbs\":[" +
                           "\"get\"" +
                         "]" +
                       "}" +
                    "]" +
                  "}";
        }
    }

}
