/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.template.PodTemplate;

/**
 * This class contains static methods which are used commonly for parsing CRDs into Models
 */
public class FromCrdUtils {
    /**
     * Parses the values from the PodDisruptionBudgetTemplate in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the PodDisruptionBudgetTemplate should be set
     * @param pdb PodDisruptionBudgetTemplate with the values form the CRD
     */
    public static void parsePodDisruptionBudgetTemplate(AbstractModel model, PodDisruptionBudgetTemplate pdb)   {
        if (pdb != null)  {
            if (pdb.getMetadata() != null) {
                model.templatePodDisruptionBudgetLabels = pdb.getMetadata().getLabels();
                model.templatePodDisruptionBudgetAnnotations = pdb.getMetadata().getAnnotations();
            }

            model.templatePodDisruptionBudgetMaxUnavailable = pdb.getMaxUnavailable();
        }
    }

    /**
     * Parses the values from the PodTemplate in CRD model into the component model
     *
     * @param model AbstractModel class where the values from the PodTemplate should be set
     * @param pod PodTemplate with the values form the CRD
     */

    public static void parsePodTemplate(AbstractModel model, PodTemplate pod)   {
        if (pod != null)  {
            if (pod.getMetadata() != null) {
                model.templatePodLabels = pod.getMetadata().getLabels();
                model.templatePodAnnotations = pod.getMetadata().getAnnotations();
            }

            model.templateTerminationGracePeriodSeconds = pod.getTerminationGracePeriodSeconds();
            model.templateImagePullSecrets = pod.getImagePullSecrets();
            model.templateSecurityContext = pod.getSecurityContext();
        }
    }
}
