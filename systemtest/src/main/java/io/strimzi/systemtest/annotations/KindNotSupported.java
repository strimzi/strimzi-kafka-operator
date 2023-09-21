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
 * Annotation to indicate that the associated test or test container should be
 * conditionally executed based on the kind supported by the KubeClusterResource.
 *
 * <p>If the kind is supported, the test will be disabled. If not, the test will be enabled.</p>
 *
 * <p>This annotation leverages the {@link KindNotSupportedCondition} to evaluate the execution condition.</p>
 *
 * @see KindNotSupportedCondition
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(KindNotSupportedCondition.class)
public @interface KindNotSupported {

    /**
     * Optional value to provide additional information or customization for the condition.
     *
     * @return the provided value or an empty string if none is set.
     */
    String value() default "";
}
