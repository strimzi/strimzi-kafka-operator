/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Represents the granting of permissions to do certain things (the verbs) to certain types of resource.
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface Permission {
    /** The types of resource being granted. */
    String[] resource();
    /** The verbs being granted. */
    String[] verbs();
}
