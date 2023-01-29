/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;
import io.fabric8.kubernetes.api.model.apps.RollingUpdateDeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.template.DeploymentTemplate;
import io.strimzi.api.kafka.model.template.PodManagementPolicy;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.api.kafka.model.template.StatefulSetTemplate;
import io.strimzi.operator.cluster.operator.resource.PodRevision;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Shared methods for creating Workload resources (Deployments, StatefulSets, StrimziPodSets)
 */
public class WorkloadUtils {
    /**
     * Create a Kubernetes Deployment with the Pod template passed as a parameter
     *
     * @param name              Name of the Deployment
     * @param namespace         Namespace of the Deployment
     * @param labels            Labels of the Deployment
     * @param ownerReference    OwnerReference of the Deployment
     * @param template          Deployment template with user's custom configuration
     * @param replicas          Number of replicas
     * @param updateStrategy    Deployment update strategy (Recreate or Rolling Update)
     * @param podTemplateSpec   The PodTemplateSpec which defines how the pods created by this Deployment look like
     *
     * @return  Created Deployment
     */
    public static Deployment createDeployment(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            DeploymentTemplate template,
            int replicas,
            DeploymentStrategy updateStrategy,
            PodTemplateSpec podTemplateSpec
    ) {
        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(TemplateUtils.annotations(template))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withStrategy(updateStrategy)
                    .withReplicas(replicas)
                    .withNewSelector()
                        .withMatchLabels(labels.strimziSelectorLabels().toMap())
                    .endSelector()
                    .withTemplate(podTemplateSpec)
                .endSpec()
                .build();
    }

    /**
     * Create a Kubernetes StatefulSet with the Pod template passed as a parameter
     *
     * @param name                      Name of the StatefulSet
     * @param namespace                 Namespace of the StatefulSet
     * @param labels                    Labels of the StatefulSet
     * @param ownerReference            OwnerReference of the StatefulSet
     * @param template                  StatefulSet template with user's custom configuration
     * @param replicas                  Number of replicas
     * @param headlessServiceName       Name of the headless service used by this StatefulSet
     * @param annotations               Additional annotations which should be set on the StatefulSet. This might
     *                                  contain annotations for tracking storage configuration, Kafka versions and similar.
     * @param volumeClaimsTemplates     PersistentVolumeClaim templates to be used in the StatefulSet
     * @param podTemplate               The PodTemplateSpec which defines how the pods created by this StatefulSet look like

     * @return  Created StatefulSet
     */
    public static StatefulSet createStatefulSet(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            StatefulSetTemplate template,
            int replicas,
            String headlessServiceName,
            Map<String, String> annotations,
            List<PersistentVolumeClaim> volumeClaimsTemplates,
            PodTemplateSpec podTemplate
    ) {
        return new StatefulSetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(annotations, TemplateUtils.annotations(template)))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withPodManagementPolicy(template != null && template.getPodManagementPolicy() != null ? template.getPodManagementPolicy().toValue() : PodManagementPolicy.PARALLEL.toValue())
                    .withNewUpdateStrategy()
                        .withType("OnDelete")
                    .endUpdateStrategy()
                    .withNewSelector()
                        .withMatchLabels(labels.strimziSelectorLabels().toMap())
                    .endSelector()
                    .withServiceName(headlessServiceName)
                    .withReplicas(replicas)
                    .withTemplate(podTemplate)
                    .withVolumeClaimTemplates(volumeClaimsTemplates)
                .endSpec()
                .build();
    }

    /**
     * Create a Strimzi PodSet with Pod definitions
     *
     * @param name           Name of the PodSet
     * @param namespace      Namespace of the PodSet
     * @param labels         Labels of the PodSet
     * @param ownerReference OwnerReference of the PodSet
     * @param template       PodSet template with user's custom configuration
     * @param replicas       Number of replicas
     * @param annotations    Additional annotations which should be set on the PodSet. This might contain annotations
     *                       for tracking storage configuration, Kafka versions and similar.
     * @param podCreator     Function for generating the Pods which should be included in this PodSet based on their
     *                       index number.
     * @return Created PodSet
     */
    public static StrimziPodSet createPodSet(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            ResourceTemplate template,
            int replicas,
            Map<String, String> annotations,
            Function<Integer, Pod> podCreator
    )  {
        List<Map<String, Object>> pods = new ArrayList<>(replicas);

        for (int i = 0; i < replicas; i++)  {
            Pod pod = podCreator.apply(i);
            pods.add(PodSetUtils.podToMap(pod));
        }

        return new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(annotations, TemplateUtils.annotations(template)))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(labels.strimziSelectorLabels().toMap()).build())
                    .addAllToPods(pods)
                .endSpec()
                .build();
    }

    /**
     * Creates a stateful Pod for use with StrimziPodSets. Stateful in this context means that it has a stable name and
     * typically uses storage.
     *
     * @param reconciliation          Reconciliation marker
     * @param name                    Name of the Pod
     * @param namespace               Namespace of the Pod
     * @param labels                  Labels of the Pod
     * @param strimziPodSetName       Name of the StrimziPodSet which is used to generate the controller labels
     * @param serviceAccountName      Name of the Service Account used by this Pod
     * @param template                Pod template with custom configurations
     * @param defaultPodLabels        The default pod labels
     * @param podAnnotations          Additional annotations used for the pod. Used to track things such as storage
     *                                configuration, Kafka versions, configuration or certificate hash stubs etc.
     * @param headlessServiceName     Name of the headless service used by this Pod
     * @param affinity                Pod's affinity
     * @param initContainers          List of init container
     * @param containers              List of main containers
     * @param volumes                 List of volumes
     * @param defaultImagePullSecrets Default image pull secrets
     * @param podSecurityContext      Pod security context
     * @return Created Pod for use with StrimziPodSet
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public static Pod createStatefulPod(
            Reconciliation reconciliation,
            String name,
            String namespace,
            Labels labels,
            String strimziPodSetName,
            String serviceAccountName,
            PodTemplate template,
            Map<String, String> defaultPodLabels,
            Map<String, String> podAnnotations,
            String headlessServiceName,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<Volume> volumes,
            List<LocalObjectReference> defaultImagePullSecrets,
            PodSecurityContext podSecurityContext
    ) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withStrimziPodName(name).withStatefulSetPod(name).withStrimziPodSetController(strimziPodSetName).withAdditionalLabels(Util.mergeLabelsOrAnnotations(defaultPodLabels, TemplateUtils.labels(template))).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(podAnnotations, TemplateUtils.annotations(template)))
                .endMetadata()
                .withNewSpec()
                    .withRestartPolicy("Always")
                    .withHostname(name)
                    .withSubdomain(headlessServiceName)
                    .withServiceAccountName(serviceAccountName)
                    .withEnableServiceLinks(template != null ? template.getEnableServiceLinks() : null)
                    .withAffinity(affinity)
                    .withInitContainers(initContainers)
                    .withContainers(containers)
                    .withVolumes(volumes)
                    .withTolerations(template != null && template.getTolerations() != null ? removeEmptyValuesFromTolerations(template.getTolerations()) : null)
                    .withTerminationGracePeriodSeconds(template != null ? (long) template.getTerminationGracePeriodSeconds() : 30L)
                    .withImagePullSecrets(imagePullSecrets(template, defaultImagePullSecrets))
                    .withSecurityContext(podSecurityContext)
                    .withPriorityClassName(template != null ? template.getPriorityClassName() : null)
                    .withSchedulerName(template != null && template.getSchedulerName() != null ? template.getSchedulerName() : "default-scheduler")
                    .withHostAliases(template != null ? template.getHostAliases() : null)
                    .withTopologySpreadConstraints(template != null ? template.getTopologySpreadConstraints() : null)
                .endSpec()
                .build();

        // Set the pod revision annotation
        pod.getMetadata().getAnnotations().put(PodRevision.STRIMZI_REVISION_ANNOTATION, PodRevision.getRevision(reconciliation, pod));

        return pod;
    }

    /**
     * Creates a Pod template for use with StatefulSet or deployment.
     *
     * @param workloadName              Name of the workload resource which will own this Pod (Deployment or StatefulSet)
     * @param labels                    Labels of the Pod
     * @param template                  Pod template with custom configurations
     * @param defaultPodLabels          The default pod labels
     * @param podAnnotations            Additional annotations used for the pod. Used to track things such as storage
     *                                  configuration, Kafka versions, configuration or certificate hash stubs etc.
     * @param affinity                  Pod's affinity
     * @param initContainers            List of init container
     * @param containers                List of main containers
     * @param volumes                   List of volumes
     * @param defaultImagePullSecrets   Default image pull secrets
     * @param podSecurityContext        Pod security context
     *
     * @return  Created Pod template for use with StatefulSet or Deployment
     */
    public static PodTemplateSpec createPodTemplateSpec(
            String workloadName,
            Labels labels,
            PodTemplate template,
            Map<String, String> defaultPodLabels,
            Map<String, String> podAnnotations,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<Volume> volumes,
            List<LocalObjectReference> defaultImagePullSecrets,
            PodSecurityContext podSecurityContext
    )   {
        return new PodTemplateSpecBuilder()
                .withNewMetadata()
                    .withLabels(labels.withAdditionalLabels(Util.mergeLabelsOrAnnotations(defaultPodLabels, TemplateUtils.labels(template))).toMap())
                    .withAnnotations(Util.mergeLabelsOrAnnotations(podAnnotations, TemplateUtils.annotations(template)))
                .endMetadata()
                .withNewSpec()
                    .withServiceAccountName(workloadName)
                    .withEnableServiceLinks(template != null ? template.getEnableServiceLinks() : null)
                    .withAffinity(affinity)
                    .withInitContainers(initContainers)
                    .withContainers(containers)
                    .withVolumes(volumes)
                    .withTolerations(template != null && template.getTolerations() != null ? removeEmptyValuesFromTolerations(template.getTolerations()) : null)
                    .withTerminationGracePeriodSeconds(template != null ? (long) template.getTerminationGracePeriodSeconds() : 30L)
                    .withImagePullSecrets(imagePullSecrets(template, defaultImagePullSecrets))
                    .withSecurityContext(podSecurityContext)
                    .withPriorityClassName(template != null ? template.getPriorityClassName() : null)
                    .withSchedulerName(template != null && template.getSchedulerName() != null ? template.getSchedulerName() : "default-scheduler")
                    .withHostAliases(template != null ? template.getHostAliases() : null)
                    .withTopologySpreadConstraints(template != null ? template.getTopologySpreadConstraints() : null)
                .endSpec()
                .build();
    }

    /**
     * Creates a Pod which can be used as a standalone pod to for example execute some task.
     *
     * @param name                      Name of the Pod
     * @param namespace                 Namespace of the Pod
     * @param labels                    Labels of the Pod
     * @param ownerReference            OwnerReference of the Pod
     * @param template                  Pod template with custom configurations
     * @param defaultPodLabels          The default pod labels
     * @param podAnnotations            Additional annotations used for the pod. Used to track things such as storage
     *                                  configuration, Kafka versions, configuration or certificate hash stubs etc.
     * @param affinity                  Pod's affinity
     * @param initContainers            List of init container
     * @param containers                List of main containers
     * @param volumes                   List of volumes
     * @param defaultImagePullSecrets   Default image pull secrets
     * @param podSecurityContext        Pod security context
     *
     * @return  Created Pod which can be used on its own
     */
    public static Pod createPod(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            PodTemplate template,
            Map<String, String> defaultPodLabels,
            Map<String, String> podAnnotations,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            List<Volume> volumes,
            List<LocalObjectReference> defaultImagePullSecrets,
            PodSecurityContext podSecurityContext
    )   {
        return new PodBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(Util.mergeLabelsOrAnnotations(defaultPodLabels, TemplateUtils.labels(template))).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(podAnnotations, TemplateUtils.annotations(template)))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withRestartPolicy("Never")
                    .withServiceAccountName(name)
                    .withEnableServiceLinks(template != null ? template.getEnableServiceLinks() : null)
                    .withAffinity(affinity)
                    .withInitContainers(initContainers)
                    .withContainers(containers)
                    .withVolumes(volumes)
                    .withTolerations(template != null && template.getTolerations() != null ? removeEmptyValuesFromTolerations(template.getTolerations()) : null)
                    .withTerminationGracePeriodSeconds(template != null ? (long) template.getTerminationGracePeriodSeconds() : 30L)
                    .withImagePullSecrets(imagePullSecrets(template, defaultImagePullSecrets))
                    .withSecurityContext(podSecurityContext)
                    .withPriorityClassName(template != null ? template.getPriorityClassName() : null)
                    .withSchedulerName(template != null && template.getSchedulerName() != null ? template.getSchedulerName() : "default-scheduler")
                    .withHostAliases(template != null ? template.getHostAliases() : null)
                    .withTopologySpreadConstraints(template != null ? template.getTopologySpreadConstraints() : null)
                .endSpec()
                .build();
    }

    /**
     * Creates the Deployment strategy for a Deployment
     *
     * @param strategy  The type of deployment strategy which should be created
     *
     * @return  Created deployment strategy
     */
    public static DeploymentStrategy deploymentStrategy(io.strimzi.api.kafka.model.template.DeploymentStrategy strategy) {
        return switch (strategy) {
            case ROLLING_UPDATE -> rollingUpdateStrategy();
            case RECREATE -> recreateStrategy();
        };
    }

    /**
     * Creates the 'Recreate' deployment strategy
     *
     * @return  Recreate deployment strategy
     */
    private static DeploymentStrategy recreateStrategy()   {
        return new DeploymentStrategyBuilder()
                .withType("Recreate")
                .build();
    }

    /**
     * Creates the Rolling update deployment strategy
     *
     * @return  Rolling update deployment strategy
     */
    private static DeploymentStrategy rollingUpdateStrategy()    {
        return new DeploymentStrategyBuilder()
                .withType("RollingUpdate")
                .withRollingUpdate(new RollingUpdateDeploymentBuilder()
                        .withMaxSurge(new IntOrString(1))
                        .withMaxUnavailable(new IntOrString(0))
                        .build())
                .build();
    }

    /**
     * If the toleration value is an empty string, set it to null. That solves an issue when built STS contains a filed
     * with an empty property value. K8s is removing properties like this, and thus we cannot fetch an equal STS which was
     * created with (some) empty value.
     *
     * @param tolerations   Tolerations list to check whether toleration value is an empty string and eventually replace it by null
     *
     * @return              List of tolerations with fixed empty strings
     */
    public static List<Toleration> removeEmptyValuesFromTolerations(List<Toleration> tolerations) {
        if (tolerations != null) {
            tolerations.stream().filter(toleration -> toleration.getValue() != null && toleration.getValue().isEmpty()).forEach(emptyValTol -> emptyValTol.setValue(null));
            return tolerations;
        } else {
            return null;
        }
    }

    /**
     * Extracts the image pull secrets configuration from the Pod template
     *
     * @param template      Pod template which maybe contains custom image pull secrets configuration
     * @param defaultValue  The default value which should be used if the image pull secrets are not set
     *
     * @return  Custom list of image pull secrets or default value if not defined
     */
    /* test */ static List<LocalObjectReference> imagePullSecrets(PodTemplate template, List<LocalObjectReference> defaultValue)  {
        return template != null && template.getImagePullSecrets() != null ? template.getImagePullSecrets() : defaultValue;
    }
}