/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * This class is a temporary work-around for the fact that Fabric8 doesn't
 * yet support an API for manipulating Kubernetes ClusterRoleBindings
 * @deprecated This can be removed once support for ClusterRoles and ClusterRoleBindings is in Fabric8.
 */
@Deprecated
public class RoleBindingOperator extends WorkaroundRbacOperator<RoleBindingOperator.RoleBinding> {

    public static final String API_VERSION = "v1beta1";
    public static final String GROUP = "rbac.authorization.k8s.io";

    public RoleBindingOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, GROUP, API_VERSION, "rolebindings");
    }

    public static class RoleBinding {
        private final String serviceAccountName;
        private final String serviceAccountNamespace;
        private final String name;
        private final String clusterRoleName;

        public RoleBinding(
                String name,
                String clusterRoleName,
                String serviceAccountNamespace, String serviceAccountName) {
            this.name = name;
            this.clusterRoleName = clusterRoleName;
            this.serviceAccountNamespace = serviceAccountNamespace;
            this.serviceAccountName = serviceAccountName;
        }
        public String toString() {
            return "{\"apiVersion\": \"" + GROUP + "/" + API_VERSION + "\"," +
                   "\"kind\": \"RoleBinding\"," +
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


    private String urlWithoutName(String namespace) {
        return baseUrl + "apis/" + group + "/" + apiVersion + "/namespaces/" + namespace + "/" + plural;
    }

    private String urlWithName(String namespace, String name) {
        return urlWithoutName(namespace) + "/" + name;
    }

    public Future<Void> reconcile(String namespace, String name, RoleBindingOperator.RoleBinding resource) {
        return doReconcile(urlWithoutName(namespace), urlWithName(namespace, name), resource);
    }

}
