/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static io.strimzi.systemtest.TestTags.REQUIRES_SHARED_NAMESPACE;

/**
 * Annotation that marks the whole test-class or test-case with {@link io.strimzi.systemtest.TestTags#REQUIRES_SHARED_NAMESPACE},
 * thanks to which the `test-suite-namespace` is created.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Tag(REQUIRES_SHARED_NAMESPACE)
public @interface RequiresSharedNamespace {
}
