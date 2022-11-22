/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for specifying the maximum value of a field in the Strimzi CRD
 */
@Repeatable(Maximum.List.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface Maximum {
    /** @return In inclusive maximum */
    int value();

    /** @return The api versions that this description applies to. */
    String apiVersions() default "all";

    /**
     * Defines several {@link Maximum} annotations on the same element.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.FIELD})
    @interface List {
        /**
         * @return  List of maximal values
         */
        Maximum[] value();
    }
}
