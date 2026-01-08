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
 * Defines the description of the field
 */
@Repeatable(Description.List.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD})
public @interface Description {
    /**
     * Gets the description in Asciidoc format.
     *
     * @return The description, in Asciidoc format.
     */
    String value();

    /**
     * Gets the API versions that this description applies to.
     *
     * @return The api versions that this description applies to.
     */
    String apiVersions() default "all";

    /**
     * Defines several {@link Description} annotations on the same element.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD})
    @interface List {
        /**
         * Gets the list of descriptions.\n
         *
         * @return  List of descriptions
         */
        Description[] value();
    }

}
