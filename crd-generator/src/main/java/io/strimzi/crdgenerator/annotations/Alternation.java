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
 * Annotation for classes which represent a choice between two or more possibilities identified by the
 * {@link Alternative}-annotated properties of the annotated class.
 * The JSON schema generated uses a {@code oneOf} constraint to force the choice between the alternative properties.
 * The annotated class will typically need to have custom Jackson serialization which can determine from the serialized
 * form which of the alternatives to deserialize.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Alternation {
}
