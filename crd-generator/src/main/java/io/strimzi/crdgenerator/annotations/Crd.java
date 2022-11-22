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
 * Some info from the top level of a {@code CustomResourceDefinition}.
 * @see <a href="https://v1-9.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#customresourcedefinition-v1beta1-apiextensions">API Reference</a>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Crd {

    /**
     * Info for the {@code spec} of the generated {@code CustomResourceDefinition}.
     * @return The spec.
     */
    Spec spec();

    /**
     * Some info from the {@code spec} part of a {@code CustomResourceDefinition}.
     */
    @Target({})
    @interface Spec {

        /**
         * @return The API group for the custom resource instances.
         */
        String group();

        /**
         * @return  The names of this CRD
         */
        Names names();

        /**
         * Configures the names of the CRD resource(s)
         */
        @Target({})
        @interface Names {
            /**
             * @return The kind of the resource
             */
            String kind();

            /**
             * @return The list kind. Defaults to ${{@linkplain #kind()}}List.
             */
            String listKind() default "";

            /**
             * @return The singular of the resource. Defaults to {@link #kind()}.
             */
            String singular() default "";

            /**
             * @return The plural of the resource.
             */
            String plural();

            /**
             * @return Short names (e.g. "svc" is the short name for the K8S "services" kind).
             */
            String[] shortNames() default {};

            /**
             * @return A list of grouped resources custom resources belong to.
             */
            String[] categories() default {};
        }

        /**
         * @return The scope of the resources. E.g. "Namespaced".
         */
        String scope();

        /**
         * @return The version of custom resources that this is the definition for.
         * @see <a href="https://v1-11.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#customresourcedefinitionversion-v1beta1-apiextensions">Kubernetes 1.11 API documtation</a>
         */
        Version[] versions() default {};

        /**
         * The version of custom resources that this is the definition for.
         * @see <a href="https://v1-11.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#customresourcedefinitionversion-v1beta1-apiextensions">Kubernetes 1.11 API documtation</a>
         */
        @interface Version {
            /**
             * @return  Name of the version
             */
            String name();

            /**
             * @return  Specifies if this version is served
             */
            boolean served();

            /**
             * @return  Specifies if this version is stored
             */
            boolean storage();
        }

        /**
         * @return The subresources of a custom resources that this is the definition for.
         * @see <a href="https://v1-11.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#customresourcedefinitionversion-v1beta1-apiextensions">Kubernetes 1.11 API documtation</a>
         */
        Subresources subresources() default @Subresources(
                status = {}
        );

        /**
         * The subresources of a custom resources that this is the definition for.
         * @see <a href="https://v1-11.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#customresourcedefinitionversion-v1beta1-apiextensions">Kubernetes 1.11 API documtation</a>
         */
        @interface Subresources {
            /**
             * @return  The status subresource configuration
             */
            Status[] status();

            /**
             * @return  The scale subresource configuration (defaults to no scale subresource)
             */
            Scale[] scale() default {};

            /**
             * The Status subresource
             */
            @interface Status {
                /**
                 * @return  The API versions in which is the status subresource supported
                 */
                String apiVersion() default "all";
            }

            /**
             * The scale subresource of a custom resources that this is the definition for.
             * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#customresourcesubresourcescale-v1beta1-apiextensions-k8s-io">Kubernetes 1.23 API documtation</a>
             */
            @interface Scale {
                /**
                 * @return  The API versions in which is the scale subresource supported
                 */
                String apiVersion() default "all";

                /**
                 * @return  The path to the desired replicas field in the spec section of the custom resource
                 */
                String specReplicasPath();

                /**
                 * @return  The path to the actual replicas field in the status section of the custom resource
                 */
                String statusReplicasPath();

                /**
                 * @return  Path to the label selector in the status section of the custom resource
                 */
                String labelSelectorPath() default "";
            }
        }

        /**
         * @return Additional printer columns.
         * @see <a href="https://v1-11.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#customresourcecolumndefinition-v1beta1-apiextensions">Kubernetes 1.11 API documtation</a>
         */
        AdditionalPrinterColumn[] additionalPrinterColumns() default {};

        /**
         * Additional printer columns.
         * @see <a href="https://v1-11.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#customresourcecolumndefinition-v1beta1-apiextensions">Kubernetes 1.11 API documtation</a>
         */
        @interface AdditionalPrinterColumn {
            /** @return The api version range in which this appears */
            String apiVersion() default "all";

            /** @return JSON path into the CR for the value to show */
            String jsonPath();

            /** @return The description of the column */
            String description();

            /**
             * One of:
             * int32
             * int64
             * float
             * double
             * byte
             * date
             * date-time
             * password
             * @return The format
             */
            String format() default "";

            /** @return The name of the column */
            String name();

            /** @return 0 to show in standard view, greater than zero to show only in wide view */
            int priority() default 0;

            /**
             * One of:
             * integer,
             * number,
             * string,
             * boolean,
             * date
             * @return The JSON type.
             */
            String type();
        }
    }
}
