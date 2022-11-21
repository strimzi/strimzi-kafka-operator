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
     * @return  API group to which the link should point
     */
    String group();

    /**
     * @return  APi version to which the link should point
     */
    String version();

    /**
     * @return  Kind to which the link should point
     */
    String kind();
}
