/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used to restrict a string to a particular regular expression.
 * <br><br>
 * When defining the regular expression, it's important to note that the string is considered valid if the expression
 * matches anywhere within the string. Unless there is a good reason to do so, it's usually less confusing to wrap the
 * regular expression in {@code ^...$}.
 * <br><br>
 * For example, the expression {@code p} will match any string containing a p, like "apple", instead the expression
 * {@code ^p$} will only match the string "p".
 */
@Repeatable(Pattern.List.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface Pattern {
    /** @return The pattern that valid values must match. */
    String value();

    /** @return The api versions that this description applies to. */
    String apiVersions() default "all";

    /**
     * Defines several {@link Pattern} annotations on the same element.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.FIELD})
    @interface List {
        /**
         * @return  List of patterns
         */
        Pattern[] value();
    }
}
