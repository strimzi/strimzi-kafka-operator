/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface OneOfType {
    /** @return List of alternatives */
    Alternative[] value();

    @interface Alternative {
        @interface Field {
            /** @return The name of a field */
            String value();
        }

        /** @return Fields in this alternative */
        Field[] value();
    }
}

