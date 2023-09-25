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
 * Annotation to indicate that a test or test class is not supported on IPv6 networks.
 *
 * <p>When a method or class is annotated with {@code @IPv6NotSupported}, it will be conditionally ignored
 * if the runtime environment is detected to be using IPv6. The actual condition logic is implemented
 * in the {@link IPv6NotSupportedCondition} class.</p>
 *
 * @see IPv6NotSupportedCondition
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(IPv6NotSupportedCondition.class)
public @interface IPv6NotSupported {

    /**
     * Optional description or reason why the test or test class is not supported on IPv6.
     *
     * @return the value describing why IPv6 is not supported.
     */
    String value() default "";
}
