/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The DescribeFile annotation indicates that given class has an additional file with Asciidoc description.
 * The file has to be placed in the documentation/book/api folder and the filename has to be a fully classified class
 * name (e.g. io.strimzi.api.kafka.MyApiClass). The asciidoc file will be included into the API reference for given
 * class.
 *
 * If the annotation is set but the file does not exist, the API reference build will fail.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DescriptionFile {
}
