/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configure the Kubernetes API to which we want to create a deep link
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface KubeLink {
    /**
     * Gets the API group to which the link should point.
     *
     * @return  API group to which the link should point
     */
    String group();

    /**
     * Gets the API version to which the link should point.
     *
     * @return  APi version to which the link should point
     */
    String version();

    /**
     * Gets the kind to which the link should point.
     *
     * @return  Kind to which the link should point
     */
    String kind();
}
