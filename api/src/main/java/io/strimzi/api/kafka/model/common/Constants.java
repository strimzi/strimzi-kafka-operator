/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.api.kafka.model.common;

public class Constants {
    public static final String RESOURCE_GROUP_NAME = "kafka.strimzi.io";
    public static final String RESOURCE_CORE_GROUP_NAME = "core.strimzi.io";

    public static final String V1 = "v1";
    public static final String V1BETA2 = "v1beta2";
    public static final String V1BETA1 = "v1beta1";
    public static final String V1ALPHA1 = "v1alpha1";

    public static final String STRIMZI_CATEGORY = "strimzi";

    public static final String FABRIC8_KUBERNETES_API = "io.fabric8.kubernetes.api.builder";

    public static final String MEMORY_REGEX = "^([0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$";
}
