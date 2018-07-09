/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Some info from the top level of a {@code CustomResourceDefinition}.
 * @see <a href="https://v1-9.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#customresourcedefinition-v1beta1-apiextensions">API Reference</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Crd {

    /**
     * The {@code apiVersion} of the generated {@code CustomResourceDefinition}.
     * (Not the {@code apiVersion} of your custom resource instances, which is {@link Spec#version()}).
     */
    String apiVersion();

    /**
     * Info for the {@code spec} of the generated {@code CustomResourceDefinition}.
     * @return
     */
    Spec spec();

    /**
     * Some info from the {@code spec} part of a {@code CustomResourceDefinition}.
     */
    @Target({})
    @interface Spec {

        /**
         * The API group for the custom resource instances
         */
        String group();

        Names names();

        @Target({})
        @interface Names {
            /**
             * The kind of the resource
             */
            String kind();

            /**
             * The list kind. Defaults to ${{@linkplain #kind()}}List.
             */
            String listKind() default "";

            /**
             * The singular of the resource. Defaults to {@link #kind()}.
             */
            String singular() default "";

            /**
             * The plural of the resource.
             */
            String plural();

            /**
             * Short names (e.g. "svc" is the short name for the K8S "services" kind).
             */
            String[] shortNames() default {};
        }

        /**
         * The scope of the resources. E.g. "Namespaced".
         */
        String scope();

        /**
         * The version of custom resources that this is the definition for.
         */
        String version();
    }

}