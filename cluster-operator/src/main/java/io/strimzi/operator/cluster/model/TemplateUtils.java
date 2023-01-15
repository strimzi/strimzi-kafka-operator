/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.template.HasMetadataTemplate;

import java.util.Map;

/**
 * Shared methods for working with Strimzi API templates
 */
public class TemplateUtils {
    /**
     * Extracts custom labels configured through the Strimzi API resource templates. This method deals the null checks
     * and makes the code using it more easy to read.
     *
     * @param template  The resource template
     *
     * @return  Map with custom labels from the template or null if not set
     */
    public static Map<String, String> labels(HasMetadataTemplate template)   {
        if (template != null
                && template.getMetadata() != null) {
            return template.getMetadata().getLabels();
        } else {
            return null;
        }
    }

    /**
     * Extracts custom annotations configured through the Strimzi API resource templates. This method deals the null
     * checks and makes the code using it more easy to read.
     *
     * @param template  The resource template
     *
     * @return  Map with custom annotations from the template or null if not set
     */
    public static Map<String, String> annotations(HasMetadataTemplate template)   {
        if (template != null
                && template.getMetadata() != null) {
            return template.getMetadata().getAnnotations();
        } else {
            return null;
        }
    }
}
