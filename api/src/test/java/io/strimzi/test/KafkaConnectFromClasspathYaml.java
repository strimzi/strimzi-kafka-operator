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
 * which causes that runner to create kafka connect clusters before, and delete kafka connect clusters after,
 * the tests.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaConnectFromClasspathYaml {

    /**
     * The name of the classpath resource, relative to the annotated element, containing the YAML.
     * The YAML should include exactly one `kind: Kafka` resource, but may also include other resources.
     * If no value is given the default resource
     * is {@code [classname].yaml} when the annotation was applied to a class,
     * or {@code [classname].[methodname].yaml} when the annotation was applied to a
     * {@code @Test}-annotated method.
     */
    String[] value() default {};
}
