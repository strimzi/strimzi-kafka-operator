/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for test classes or methods using {@code @ExtendWith(StrimziExtension.class)}
 * which enables to create a cluster operator before, and delete a cluster operator after,
 * the tests.
 */
@Deprecated
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ClusterOperator {

    EnvVariables[] envVariables() default {};
    boolean useHelmChart() default false;
}
