/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

/**
 * Abstraction for a kubernetes client.
 */
public interface KubeClient {
    void createRole(String roleName, Permission... permissions);

    void createRoleBinding(String bindingName, String roleName, String... users);

    void deleteRoleBinding(String bindingName);

    void deleteRole(String roleName);

    String defaultNamespace();

    boolean clientAvailable();
}
