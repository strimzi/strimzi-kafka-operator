/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package example;

import io.strimzi.docgen.annotations.Table;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Table
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.FIELD})
@Documented
public @interface EnvVar {
    @Table.Column(heading = "Variable Name")
    String name();

    @Table.Column(heading = "Description")
    String doc();

    @Table.Column(heading = "Default")
    String defaultValue();

    @Table.Column(heading = "Mandatory")
    boolean required() default false;
}

