/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplate;
import io.strimzi.operator.common.model.Labels;

/**
 * Shared methods for working with PodDisruptionBudgets (PDB)
 */
public class PodDisruptionBudgetUtils {
    private static final int DEFAULT_MAX_UNAVAILABLE = 1;

    /**
     * Creates the PodDisruptionBudget
     *
     * @param name              Name of the PDB
     * @param namespace         Namespace of the PDB
     * @param labels            Labels of the PDB
     * @param ownerReference    OwnerReference of the PDB
     * @param template          PDB template with user's custom configuration
     *
     * @return The generated PodDisruptionBudget
     */
    public static PodDisruptionBudget createPodDisruptionBudget(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            PodDisruptionBudgetTemplate template
    )   {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(TemplateUtils.annotations(template))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(template != null ? template.getMaxUnavailable() : DEFAULT_MAX_UNAVAILABLE)
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(labels.strimziSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }

    /**
     * Creates the v1beta1 PodDisruptionBudget
     *
     * @param name              Name of the PDB
     * @param namespace         Namespace of the PDB
     * @param labels            Labels of the PDB
     * @param ownerReference    OwnerReference of the PDB
     * @param template          PDB template with user's custom configuration
     *
     * @return The generated v1beta1 PodDisruptionBudget
     */
    public static io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget createPodDisruptionBudgetV1Beta1(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            PodDisruptionBudgetTemplate template
    )   {
        return new io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(TemplateUtils.annotations(template))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withNewMaxUnavailable(template != null ? template.getMaxUnavailable() : DEFAULT_MAX_UNAVAILABLE)
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(labels.strimziSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }

    /**
     * Creates a PodDisruptionBudget for use with custom pod controller (such as StrimziPodSetController). Unlike
     * built-in controllers (Deployments, StatefulSets, Jobs, DaemonSets, ...), custom pod controllers can use only PDBs
     * with minAvailable in absolute numbers (i.e. no percentages).
     * <p>
     * See https://kubernetes.io/docs/tasks/run-application/configure-pdb/#arbitrary-controllers-and-selectors for more
     * details.
     *
     * @param name              Name of the PDB
     * @param namespace         Namespace of the PDB
     * @param labels            Labels of the PDB
     * @param ownerReference    OwnerReference of the PDB
     * @param template          PDB template with user's custom configuration
     * @param replicas          Number of replicas in the resource(s) which this PDB covers
     *
     * @return The generated PodDisruptionBudget
     */
    public static PodDisruptionBudget createCustomControllerPodDisruptionBudget(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            PodDisruptionBudgetTemplate template,
            int replicas
    )   {
        return new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(TemplateUtils.annotations(template))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withMinAvailable(new IntOrString(replicas - (template != null ? template.getMaxUnavailable() : DEFAULT_MAX_UNAVAILABLE)))
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(labels.strimziSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }

    /**
     * Creates a PodDisruptionBudget V1Beta1 for use with custom pod controller (such as StrimziPodSetController). Unlike
     * built-in controllers (Deployments, StatefulSets, Jobs, DaemonSets, ...), custom pod controllers can use only PDBs
     * with minAvailable in absolute numbers (i.e. no percentages).
     * <p>
     * See https://kubernetes.io/docs/tasks/run-application/configure-pdb/#arbitrary-controllers-and-selectors for more
     * details.
     *
     * @param name              Name of the PDB
     * @param namespace         Namespace of the PDB
     * @param labels            Labels of the PDB
     * @param ownerReference    OwnerReference of the PDB
     * @param template          PDB template with user's custom configuration
     * @param replicas          Number of replicas in the resource(s) which this PDB covers
     *
     * @return The generated v1beta1 PodDisruptionBudget
     */
    public static io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget createCustomControllerPodDisruptionBudgetV1Beta1(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            PodDisruptionBudgetTemplate template,
            int replicas
    )   {
        return new io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(TemplateUtils.annotations(template))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withMinAvailable(new IntOrString(replicas - (template != null ? template.getMaxUnavailable() : DEFAULT_MAX_UNAVAILABLE)))
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(labels.strimziSelectorLabels().toMap()).build())
                .endSpec()
                .build();
    }
}
