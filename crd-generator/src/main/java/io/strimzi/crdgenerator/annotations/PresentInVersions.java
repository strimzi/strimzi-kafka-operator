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
 * Used when a property is present from or until a particular CR API version. It can be also used on a type definition,
 * in which case the type will be included only in some API versions when it is used in some typed APIs (based on the
 * "type: xxx" differentiator).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.TYPE})
public @interface PresentInVersions {
    /**
     * @return The versions in which the annotated property is present
     */
    String value();
}
