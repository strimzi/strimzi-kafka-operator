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
         * Gets the API group for the custom resource instances.
         *
         * @return The API group for the custom resource instances.
         */
        String group();

        /**
         * Gets the names of this CRD.
         *
         * @return  The names of this CRD
         */
        Names names();

        /**
         * Configures the names of the CRD resource(s)
         */
        @Target({})
        @interface Names {
            /**
             * Gets the kind of the resource.
             *
             * @return The kind of the resource
             */
            String kind();

            /**
             * Gets the list kind of the resource.
             *
             * @return The list kind. Defaults to ${{@linkplain #kind()}}List.
             */
            String listKind() default "";

            /**
             * Gets the singular of the resource.
             *
             * @return The singular of the resource. Defaults to {@link #kind()}.
             */
            String singular() default "";

            /**
             * Gets the plural of the resource.
             *
             * @return The plural of the resource.
             */
            String plural();

            /**
             * Gets short names for the resource.
             *
             * @return Short names (e.g. "svc" is the short name for the K8S "services" kind).
             */
            String[] shortNames() default {};

            /**
             * Gets grouped resource categories.
             *
             * @return A list of grouped resources custom resources belong to.
             */
            String[] categories() default {};
        }

        /**
         * Gets the scope of the resources.
         *
         * @return The scope of the resources. E.g. "Namespaced".
         */
        String scope();

        /**
         * Gets the versions of custom resources that this is the definition for.
         *
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
             * Gets the name of the version.
             *
             * @return  Name of the version
             */
            String name();

            /**
             * Checks if this version is served.
             *
             * @return  Specifies if this version is served
             */
            boolean served();

            /**
             * Checks if this version is stored.
             *
             * @return  Specifies if this version is stored
             */
            boolean storage();

            /**
             * Checks if this version is deprecated.
             *
             * @return  Specifies if this version is deprecated
             */
            boolean deprecated() default false;

            /**
             * Gets the deprecation warning message.
             *
             * @return  Specifies the deprecation warning. Should be used only for deprecated resource versions.
             */
            String deprecationWarning() default "";
        }

        /**
         * Gets the subresources of a custom resources.
         *
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
             * Gets the status subresource configuration.
             *
             * @return  The status subresource configuration
             */
            Status[] status();

            /**
             * Gets the scale subresource configuration.
             *
             * @return  The scale subresource configuration (defaults to no scale subresource)
             */
            Scale[] scale() default {};

            /**
             * The Status subresource
             */
            @interface Status {
                /**
                 * Gets the API versions for status subresource.
                 *
                 * @return  The API versions in which is the status subresource supported
                 */
                String apiVersion() default "all";
            }

            /**
             * The scale subresource of a custom resources that this is the definition for.
             * @see <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#customresourcesubresourcescale-v1-apiextensions-k8s-io">Kubernetes 1.30 API documetation</a>
             */
            @interface Scale {
                /**
                 * Gets the API versions for scale subresource.
                 *
                 * @return  The API versions in which is the scale subresource supported
                 */
                String apiVersion() default "all";

                /**
                 * Gets the path to the desired replicas field in the spec section.
                 *
                 * @return  The path to the desired replicas field in the spec section of the custom resource
                 */
                String specReplicasPath();

                /**
                 * Gets the path to the actual replicas field in the status section.
                 *
                 * @return  The path to the actual replicas field in the status section of the custom resource
                 */
                String statusReplicasPath();

                /**
                 * Gets the path to the label selector in the status section.
                 *
                 * @return  Path to the label selector in the status section of the custom resource
                 */
                String labelSelectorPath() default "";
            }
        }

        /**
         * Gets the additional printer columns.
         *
         * @return Additional printer columns.
         * @see <a href="https://v1-11.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#customresourcecolumndefinition-v1beta1-apiextensions">Kubernetes 1.11 API documtation</a>
         */
        AdditionalPrinterColumn[] additionalPrinterColumns() default {};

        /**
         * Additional printer columns.
         * @see <a href="https://v1-11.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#customresourcecolumndefinition-v1beta1-apiextensions">Kubernetes 1.11 API documtation</a>
         */
        @interface AdditionalPrinterColumn {
            /**
             * Gets the API version range for this column.
             *
             * @return The api version range in which this appears
             */
            String apiVersion() default "all";

            /**
             * Gets the JSON path to the value to show.
             *
             * @return JSON path into the CR for the value to show
             */
            String jsonPath();

            /**
             * Gets the description of the column.
             *
             * @return The description of the column
             */
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

            /**
             * Gets the name of the column.
             *
             * @return The name of the column
             */
            String name();

            /**
             * Gets the priority of the column.
             *
             * @return 0 to show in standard view, greater than zero to show only in wide view
             */
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
