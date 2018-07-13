/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Vertx;

/**
 * This class is a temporary work-around for the fact that Fabric8 doesn't
 * yet support an API for manipulating Kubernetes ClusterRoleBindings
 * @deprecated This can be removed once support for ClusterRoles and ClusterRoleBindings is in Fabric8.
 */
@Deprecated
public class ClusterRoleBindingOperator extends WorkaroundRbacOperator<ClusterRoleBindingOperator.ClusterRoleBinding> {

    public ClusterRoleBindingOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "rbac.authorization.k8s.io", "v1beta1", "clusterrolebindings");
    }

    public static class ClusterRoleBinding {
        private final String serviceAccountName;
        private final String serviceAccountNamespace;
        private final String name;
        private final String clusterRoleName;

        public ClusterRoleBinding(
                String name,
                String clusterRoleName,
                String serviceAccountNamespace, String serviceAccountName) {
            this.name = name;
            this.clusterRoleName = clusterRoleName;
            this.serviceAccountNamespace = serviceAccountNamespace;
            this.serviceAccountName = serviceAccountName;
        }
        public String toString() {
            return "{\"apiVersion\": \"rbac.authorization.k8s.io/v1beta1\"," +
                   "\"kind\": \"ClusterRoleBinding\"," +
                   "\"metadata\":{" +
                   "  \"name\": \"" + name + "\"," +
                   "  \"labels\":{" +
                   "    \"app\": \"strimzi\"" +
                    "}" +
                    "}," +
                   "\"subjects\":[" +
                   "  { \"kind\": \"ServiceAccount\"," +
                   "    \"name\": \"" + serviceAccountName + "\"," +
                   "    \"namespace\": \"" + serviceAccountNamespace + "\"" +
                    "}" +
                    "]," +
                   "\"roleRef\":{" +
                   "  \"kind\": \"ClusterRole\"," +
                   "  \"name\": \"" + clusterRoleName + "\"," +
                   "  \"apiGroup\": \"rbac.authorization.k8s.io\"}" +
                    "}";
        }
    }

}
