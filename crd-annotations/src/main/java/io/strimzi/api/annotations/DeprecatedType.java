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
 * Annotation to identify a deprecated type in the Strimzi CRDs / API
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface DeprecatedType {
    /**
     * @return The type which should be used as replacement
     */
    Class<?> replacedWithType();

    /**
     * @return The API version in which this property is scheduled to be removed.
     */
    String removalVersion() default "";
}
