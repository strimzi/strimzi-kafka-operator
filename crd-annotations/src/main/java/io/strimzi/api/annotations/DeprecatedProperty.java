/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to identify a deprecated property in the Strimzi CRDs / API
 */
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DeprecatedProperty {
    /**
     * Gets the API version in which this property is scheduled to be removed.
     *
     * @return The API version in which this property is scheduled to be removed.
     */
    String removalVersion() default "";

    /**
     * Gets the new path if this property has moved.
     *
     * @return If this property has moved to a different location in the Custom Resource this is
     * the path it has moved to.
     */
    String movedToPath() default "";

    /**
     * Gets the description of how to configure the functionality if the property has not moved.
     *
     * @return If this property has <strong>not</strong> moved to a different location in the Custom Resource this is
     * a description of how the functionality can now be configured.
     */
    String description() default "";
}
