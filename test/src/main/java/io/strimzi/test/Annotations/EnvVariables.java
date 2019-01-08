/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.Annotations;

import io.strimzi.test.Extensions.StrimziExtension;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation is used in ({@link ClusterOperator})
 * to configure a Cluster Operator with custom environment variables.
 * <p>
 * An example would be:
 * <pre>
 * &#064;ExtendedWith({@link StrimziExtension})
 * &#064;ClusterOperator(envVariables = {
 * &#064;EnvVariables(key = "foo", value = "bar")
 * })
 * public class ClusterTest {
 * }
 * </pre>
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface EnvVariables {

    String key();
    String value();
}
