/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Describes a Kubernetes RBAC {@code RoleBinding}.
 * @see <a href="https://kubernetes.io/docs/admin/authorization/rbac">Kubernetes RBAC docs</a>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface RoleBinding {
    /** The name of the role binding. If not specified it will default to the Role name + "-binding". */
    String name();
    /** The name of the @Role being bound. */
    String role();
    /** The users being granted the role. */
    String[] users();
}
