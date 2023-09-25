/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to indicate that a JUnit test or test class is not supported when running on a 'kind' K8s cluster
 * with IPv6 due to the lack of support.
 * <p>
 * Tests or classes annotated with this will be skipped during execution if the environment is using a 'kind'
 * K8s cluster with IPv6.
 * </p>
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(KindNotSupportedCondition.class)
public @interface KindIPv6NotSupported {

    /**
     * Optional value to provide additional information or reason for disabling the test.
     *
     * @return      A string indicating the reason or additional info.
     */
    String value() default "";
}
