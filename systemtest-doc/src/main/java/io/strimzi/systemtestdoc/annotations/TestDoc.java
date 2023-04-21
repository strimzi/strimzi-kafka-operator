/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtestdoc.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface TestDoc {

    Desc description();
    Step[] steps();
    Usecase[] usecases();

    @interface Desc {
        String value();
    }

    @interface Step {
        String value();
        String expected();
    }

    @interface Usecase {
        String id();
        String note() default "";
    }
}
