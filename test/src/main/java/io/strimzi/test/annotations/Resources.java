/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for test classes or methods using {@code @ExtendWith(StrimziExtension.class)}
 * which enables to create resources before, and delete resources after,
 * the tests.
 */
@Deprecated
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Resources.Container.class)
public @interface Resources {

    /**
     * Paths to YAML resource files/directories. Directories are processed recursively,
     * depth-first, with files in lexicographic order.
     */
    String[] value();

    boolean asAdmin() default false;

    @Target({ElementType.METHOD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @interface Container {
        Resources[] value();
    }
}
