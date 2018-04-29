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
 * Annotation is intended for test methods for the ability to group test cases and execute certain types of groups on CI.
 * For using this annotation, need to define an instance of ({@link JUnitGroupRule}) as a ({@link org.junit.Rule})
 * for each test classes.
 *
 * Availible types - acceptance, regression
 *
 * How to execute the expected group of system tests. Add system property "junitgroup" with following value:
 * -Djunitgroup=integration - to execute one test group
 * -Djunitgroup=acceptance,regression - to execute many test groups
 * -Djunitgroup=all - to execute all test groups
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface JUnitGroup {

    String DEFAULT_KEY = "junitgroup";
    String ALL_GROUPS = "all";

    /** The name of the system property that allows to test groups. The default is {@value #DEFAULT_KEY}. */
    String key() default DEFAULT_KEY;

    /**
     * Name of the test group(s) to which a test belongs. If the name is not defined, the test will belong to the default
     * implicit group.
     */
    String[] name() default {};
}
