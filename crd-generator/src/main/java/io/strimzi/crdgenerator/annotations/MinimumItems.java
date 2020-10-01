/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**    {@link MinimumItems}
 *     A inclusive minimum for size of the array.
 *     This gets added to the {@code minItems}
 *     of {@code property}s within the corresponding Schema Object.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface MinimumItems {
    int value();
}
