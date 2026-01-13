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
 * Annotation for configuring CEL validation rules in Strimzi CRDs. The CEL validation rules should be added only with
 * backwards compatibility in mind in order to not make previously valid CRs invalid. For example, you can use it to
 * make previously required field required only in some condition. But should not use it to make a previously optional
 * field required under some conditions.
 *
 * fieldPath and reason fields are support only on Kubernetes 1.28 and newer.
 *
 * For more details see https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation-rules
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface CelValidation {
    /**
     * Gets the CEL validation rules that should be applied to this type.
     *
     * @return  CEL validation rules that should be applied to this type
     */
    CelValidationRule[] rules() default {};

    /**
     * CEL Validation rule
     */
    @interface CelValidationRule {
        /**
         * Gets the validation rule.
         *
         * @return  Validation rule
         */
        String rule();

        /**
         * Gets the error message.
         *
         * @return  Error message
         */
        String message() default "";

        /**
         * Gets the error message expression.
         *
         * @return  Error message expression
         */
        String messageExpression() default "";

        /**
         * Gets the error reason.
         *
         * @return  Error reason
         */
        String reason() default "";

        /**
         * Gets the field path.
         *
         * @return  Return fieldPath
         */
        String fieldPath() default "";

        /**
         * Gets the versions in which this validation rule is used.
         *
         * @return The versions in which this validation rule is used
         */
        String versions() default "";
    }
}
