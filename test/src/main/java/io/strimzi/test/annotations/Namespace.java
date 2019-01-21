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
 * which enables to create namespaces before, and delete namespaces after,
 * the tests.
 */
@Deprecated
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(Namespace.Container.class)
public @interface Namespace {

    /** The name of the namespace to create/delete. */
    String value();

    /**
     * Whether the KubeClient should be switched to the new namespace
     */
    boolean use() default true;

    @Target({ElementType.METHOD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @interface Container {
        Namespace[] value();
    }

}
