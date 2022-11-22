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
 * Annotation for configuring fields from which cannot be used both together
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface OneOf {
    /** @return List of alternatives */
    Alternative[] value();

    /**
     * Defines the alternative
     */
    @interface Alternative {
        /**
         * Defines the alternative property / field
         */
        @interface Property {
            /** @return The name of a property */
            String value();
            /** @return Whether this property is required */
            boolean required() default true;
        }

        /** @return Properties in this alternative */
        Property[] value();
    }
}

