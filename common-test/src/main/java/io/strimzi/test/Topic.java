/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for test classes or methods run via {@code @RunWith(StrimziRunner.class)}
 * which causes that runner create kafka config map with Topic before,
 * and delete kafka config map with Topic after the tests.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Topic {

    String name();
    String clusterName();
    int partitions() default 1;
    int replicas() default 1;

    @Target({ElementType.METHOD, ElementType.TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @interface Container {
        Topic[] value();
    }
}
