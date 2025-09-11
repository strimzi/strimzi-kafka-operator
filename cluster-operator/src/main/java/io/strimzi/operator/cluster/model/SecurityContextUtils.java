/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.strimzi.api.kafka.model.common.template.PodTemplate;

/**
 * Utility class for handling security context inheritance and merging
 * NOTE: This is optional - consider if you really need this complexity
 */
public class SecurityContextUtils {

    private SecurityContextUtils() {
        // Utility class
    }

    /**
     * Creates a container security context by inheriting from pod-level security context.
     * Only inherits compatible fields that apply to containers.
     *
     * @param podTemplate Pod template containing pod-level security context
     * @return Container security context with inherited values, or null if no pod context
     */
    public static SecurityContext inheritFromPod(PodTemplate podTemplate) {
        if (podTemplate == null || podTemplate.getSecurityContext() == null) {
            return null;
        }

        PodSecurityContext podSecurityContext = podTemplate.getSecurityContext();
        SecurityContextBuilder builder = new SecurityContextBuilder();
        boolean hasAnyValue = false;

        // Inherit fields that apply to containers
        if (podSecurityContext.getRunAsUser() != null) {
            builder.withRunAsUser(podSecurityContext.getRunAsUser());
            hasAnyValue = true;
        }

        if (podSecurityContext.getRunAsGroup() != null) {
            builder.withRunAsGroup(podSecurityContext.getRunAsGroup());
            hasAnyValue = true;
        }

        if (podSecurityContext.getRunAsNonRoot() != null) {
            builder.withRunAsNonRoot(podSecurityContext.getRunAsNonRoot());
            hasAnyValue = true;
        }

        if (podSecurityContext.getSeLinuxOptions() != null) {
            builder.withSeLinuxOptions(podSecurityContext.getSeLinuxOptions());
            hasAnyValue = true;
        }

        if (podSecurityContext.getSeccompProfile() != null) {
            builder.withSeccompProfile(podSecurityContext.getSeccompProfile());
            hasAnyValue = true;
        }

        // Only return a context if we actually inherited something
        return hasAnyValue ? builder.build() : null;
    }
}