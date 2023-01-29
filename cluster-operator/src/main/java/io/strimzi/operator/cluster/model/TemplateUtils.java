/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.template.DeploymentStrategy;
import io.strimzi.api.kafka.model.template.DeploymentTemplate;
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

    /**
     * Extracts the deployment strategy configuration from the Deployment template
     *
     * @param template      Deployment template which maybe contains custom deployment strategy configuration
     * @param defaultValue  The default value which should be used if the deployment strategy is not set
     *
     * @return  Custom deployment strategy or default value if not defined
     */
    public static DeploymentStrategy deploymentStrategy(DeploymentTemplate template, DeploymentStrategy defaultValue)  {
        return template != null && template.getDeploymentStrategy() != null ? template.getDeploymentStrategy() : defaultValue;
    }
}
