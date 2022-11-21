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
 * A inclusive minimum for size of an array/List-typed property.
 * This gets added to the {@code minItems}
 * of {@code property}s within the corresponding Schema Object.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface MinimumItems {
    /**
     * @return The api versions that this description applies to.
     **/
    String apiVersions() default "all";

    /**
     * @return  Minimum number of items in this list / array
     */
    int value();

    /**
     * Defines several {@link MinimumItems} annotations on the same element.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.FIELD})
    @interface List {
        /**
         * @return  List of minimum number of items
         */
        MinimumItems[] value();
    }
}
