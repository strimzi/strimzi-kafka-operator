/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.HostAliasBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodDNSConfig;
import io.fabric8.kubernetes.api.model.PodDNSConfigBuilder;
import io.fabric8.kubernetes.api.model.PodDNSConfigOptionBuilder;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategy;
import io.strimzi.api.kafka.model.common.template.DeploymentTemplateBuilder;
import io.strimzi.api.kafka.model.common.template.DnsPolicy;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplateBuilder;
import io.strimzi.api.kafka.model.common.template.ResourceTemplateBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
public class WorkloadUtilsTest {
    private final static String NAME = "my-workload";
    private final static String NAMESPACE = "my-namespace";
    private final static String HEADLESS_SERVICE_NAME = "my-workload-headless";
    private final static int REPLICAS = 5;
    private final static Set<NodeRef> NODES = Set.of(
            new NodeRef("my-cluster-nodes-10", 10, "nodes", false, true),
            new NodeRef("my-cluster-nodes-11", 11, "nodes", false, true),
            new NodeRef("my-cluster-nodes-12", 12, "nodes", false, true)
    );
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-workload")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type")
            .withAdditionalLabels(Map.of("label-1", "value-1", "label-2", "value-2"));
    private static final PodTemplateSpec DUMMY_POD_TEMPLATE_SPEC = new PodTemplateSpecBuilder()
            .withNewMetadata()
                .withLabels(Map.of("dummy", "label"))
                .withAnnotations(Map.of("dummy", "anno"))
            .endMetadata()
            .withNewSpec()
                .withContainers(new Container())
            .endSpec()
            .build();
    private static final Storage DEFAULT_STORAGE = new PersistentClaimStorageBuilder().withSize("100Gi").build();
    private static final Affinity DEFAULT_AFFINITY = new AffinityBuilder()
            .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                    .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                        .addNewMatchExpression()
                            .withKey("key1")
                            .withOperator("In")
                            .withValues("value1", "value2")
                        .endMatchExpression()
                        .build())
                .endRequiredDuringSchedulingIgnoredDuringExecution()
            .endNodeAffinity()
            .build();
    private static final Toleration DEFAULT_TOLERATION = new TolerationBuilder()
            .withEffect("NoExecute")
            .withKey("key1")
            .withOperator("Equal")
            .withValue("value1")
            .build();
    private static final TopologySpreadConstraint DEFAULT_TOPOLOGY_SPREAD_CONSTRAINT = new TopologySpreadConstraintBuilder()
            .withTopologyKey("kubernetes.io/zone")
            .withMaxSkew(1)
            .withWhenUnsatisfiable("DoNotSchedule")
            .withLabelSelector(new LabelSelectorBuilder().withMatchLabels(singletonMap("label", "value")).build())
            .build();
    private static final PodSecurityContext DEFAULT_POD_SECURITY_CONTEXT = new PodSecurityContextBuilder()
            .withFsGroup(123L)
            .withRunAsGroup(456L)
            .withRunAsUser(789L)
            .build();
    private static final HostAlias DEFAULT_HOST_ALIAS = new HostAliasBuilder()
            .withIp("127.0.0.1")
            .withHostnames("home")
            .build();
    private static final DnsPolicy DEFAULT_DNS_POLICY = DnsPolicy.NONE;
    private static final PodDNSConfig DEFAULT_DNS_CONFIG = new PodDNSConfigBuilder()
            .withNameservers("192.0.2.1")
            .withSearches("ns1.svc.cluster-domain.example", "my.dns.search.suffix")
            .withOptions(
                new PodDNSConfigOptionBuilder()
                        .withName("ndots")
                        .withValue("2")
                        .build(), 
                new PodDNSConfigOptionBuilder()
                        .withName("edns0")
                        .build()
            )
            .build();

    //////////////////////////////////////////////////
    // Deployment tests
    //////////////////////////////////////////////////

    @Test
    public void testCreateDeploymentWithNullTemplateAndRecreateStrategy()  {
        Deployment dep = WorkloadUtils.createDeployment(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                null,
                REPLICAS,
                Map.of("extra", "annotations"),
                WorkloadUtils.deploymentStrategy(io.strimzi.api.kafka.model.common.template.DeploymentStrategy.RECREATE),
                DUMMY_POD_TEMPLATE_SPEC
        );

        assertThat(dep.getMetadata().getName(), is(NAME));
        assertThat(dep.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(dep.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(dep.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(dep.getMetadata().getAnnotations(), is(Map.of("extra", "annotations")));

        assertThat(dep.getSpec().getStrategy().getType(), is("Recreate"));
        assertThat(dep.getSpec().getReplicas(), is(REPLICAS));
        assertThat(dep.getSpec().getTemplate(), is(DUMMY_POD_TEMPLATE_SPEC));
        assertThat(dep.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(dep.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(dep.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-workload"));
        assertThat(dep.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    @Test
    public void testCreateDeploymentWithTemplateAndRollingUpdateStrategy()  {
        Deployment dep = WorkloadUtils.createDeployment(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                new DeploymentTemplateBuilder()
                        .withNewMetadata()
                            .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                            .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                        .endMetadata()
                        .build(),
                REPLICAS,
                Map.of("extra", "annotations"),
                WorkloadUtils.deploymentStrategy(io.strimzi.api.kafka.model.common.template.DeploymentStrategy.ROLLING_UPDATE),
                DUMMY_POD_TEMPLATE_SPEC
        );

        assertThat(dep.getMetadata().getName(), is(NAME));
        assertThat(dep.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(dep.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(dep.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(dep.getMetadata().getAnnotations(), is(Map.of("extra", "annotations", "anno-1", "value-1", "anno-2", "value-2")));

        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));
        assertThat(dep.getSpec().getReplicas(), is(REPLICAS));
        assertThat(dep.getSpec().getTemplate(), is(DUMMY_POD_TEMPLATE_SPEC));
        assertThat(dep.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(dep.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(dep.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-workload"));
        assertThat(dep.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    //////////////////////////////////////////////////
    // StrimziPodSet tests
    //////////////////////////////////////////////////

    @Test
    public void testCreateStrimziPodSetWithNullTemplate()  {
        List<Integer> podIds = new ArrayList<>();

        StrimziPodSet sps = WorkloadUtils.createPodSet(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                null,
                REPLICAS,
                Map.of("extra", "annotations"),
                LABELS.strimziSelectorLabels(),
                i -> {
                    podIds.add(i);
                    return new PodBuilder()
                            .withNewMetadata()
                                .withName(NAME + "-" + i)
                            .endMetadata()
                            .build();
                }
        );

        assertThat(sps.getMetadata().getName(), is(NAME));
        assertThat(sps.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(sps.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(sps.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(sps.getMetadata().getAnnotations(), is(Map.of("extra", "annotations")));

        assertThat(sps.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(sps.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(sps.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-workload"));
        assertThat(sps.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));

        // Test generating pods from the PodCreator method
        assertThat(podIds.size(), is(5));
        assertThat(podIds, is(List.of(0, 1, 2, 3, 4)));
        assertThat(sps.getSpec().getPods().size(), is(5));
        assertThat(sps.getSpec().getPods().stream().map(pod -> PodSetUtils.mapToPod(pod).getMetadata().getName()).toList(), is(List.of("my-workload-0", "my-workload-1", "my-workload-2", "my-workload-3", "my-workload-4")));
    }

    @Test
    public void testCreateStrimziPodSetWithTemplate()  {
        List<Integer> podIds = new ArrayList<>();

        StrimziPodSet sps = WorkloadUtils.createPodSet(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                new ResourceTemplateBuilder()
                        .withNewMetadata()
                            .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                            .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                        .endMetadata()
                        .build(),
                REPLICAS,
                Map.of("extra", "annotations"),
                Labels.fromMap(Map.of("custom", "selector")),
                i -> {
                    podIds.add(i);
                    return new PodBuilder()
                            .withNewMetadata()
                                .withName(NAME + "-" + i)
                            .endMetadata()
                            .build();
                }
        );

        assertThat(sps.getMetadata().getName(), is(NAME));
        assertThat(sps.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(sps.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(sps.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(sps.getMetadata().getAnnotations(), is(Map.of("extra", "annotations", "anno-1", "value-1", "anno-2", "value-2")));

        assertThat(sps.getSpec().getSelector().getMatchLabels().size(), is(1));
        assertThat(sps.getSpec().getSelector().getMatchLabels(), is(Map.of("custom", "selector")));

        // Test generating pods from the PodCreator method
        assertThat(podIds.size(), is(5));
        assertThat(podIds, is(List.of(0, 1, 2, 3, 4)));
        assertThat(sps.getSpec().getPods().size(), is(5));
        assertThat(sps.getSpec().getPods().stream().map(pod -> PodSetUtils.mapToPod(pod).getMetadata().getName()).toList(), is(List.of("my-workload-0", "my-workload-1", "my-workload-2", "my-workload-3", "my-workload-4")));
    }

    // Tests with node references instead of number of replicas
    @Test
    public void testCreateStrimziPodSetFromNodeReferencesWithNullTemplate()  {
        List<String> podNames = new ArrayList<>();

        StrimziPodSet sps = WorkloadUtils.createPodSet(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                null,
                NODES,
                Map.of("extra", "annotations"),
                LABELS.strimziSelectorLabels(),
                n -> {
                    podNames.add(n.podName());
                    return new PodBuilder()
                            .withNewMetadata()
                                .withName(n.podName())
                            .endMetadata()
                            .build();
                }
        );

        assertThat(sps.getMetadata().getName(), is(NAME));
        assertThat(sps.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(sps.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(sps.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(sps.getMetadata().getAnnotations(), is(Map.of("extra", "annotations")));

        assertThat(sps.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(sps.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(sps.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-workload"));
        assertThat(sps.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));

        // Test generating pods from the PodCreator method
        assertThat(podNames.size(), is(3));
        assertThat(podNames, hasItems("my-cluster-nodes-10", "my-cluster-nodes-11", "my-cluster-nodes-12"));
        assertThat(sps.getSpec().getPods().size(), is(3));
        assertThat(sps.getSpec().getPods().stream().map(pod -> PodSetUtils.mapToPod(pod).getMetadata().getName()).toList(), hasItems("my-cluster-nodes-10", "my-cluster-nodes-11", "my-cluster-nodes-12"));
    }

    @Test
    public void testCreateStrimziPodSetFromNodeReferencesWithTemplate()  {
        List<String> podNames = new ArrayList<>();

        StrimziPodSet sps = WorkloadUtils.createPodSet(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                new ResourceTemplateBuilder()
                        .withNewMetadata()
                            .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                            .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                        .endMetadata()
                        .build(),
                NODES,
                Map.of("extra", "annotations"),
                Labels.fromMap(Map.of("custom", "selector")),
                n -> {
                    podNames.add(n.podName());
                    return new PodBuilder()
                            .withNewMetadata()
                            .withName(n.podName())
                            .endMetadata()
                            .build();
                }
        );

        assertThat(sps.getMetadata().getName(), is(NAME));
        assertThat(sps.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(sps.getMetadata().getOwnerReferences(), is(List.of(ResourceUtils.DUMMY_OWNER_REFERENCE)));
        assertThat(sps.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(sps.getMetadata().getAnnotations(), is(Map.of("extra", "annotations", "anno-1", "value-1", "anno-2", "value-2")));

        assertThat(sps.getSpec().getSelector().getMatchLabels().size(), is(1));
        assertThat(sps.getSpec().getSelector().getMatchLabels(), is(Map.of("custom", "selector")));

        // Test generating pods from the PodCreator method
        assertThat(podNames.size(), is(3));
        assertThat(podNames, hasItems("my-cluster-nodes-10", "my-cluster-nodes-11", "my-cluster-nodes-12"));
        assertThat(sps.getSpec().getPods().size(), is(3));
        assertThat(sps.getSpec().getPods().stream().map(pod -> PodSetUtils.mapToPod(pod).getMetadata().getName()).toList(), hasItems("my-cluster-nodes-10", "my-cluster-nodes-11", "my-cluster-nodes-12"));
    }

    @Test
    public void testPatchPodAnnotations() {
        Map<String, String> annotations = Map.of("anno-1", "value-1", "anno-2", "value-2", "anno-3", "value-3");
        List<Pod> pods = new ArrayList<>();
        pods.add(new PodBuilder()
                .withNewMetadata()
                    .withName("pod-0")
                    .withNamespace(NAMESPACE)
                    .withAnnotations(annotations)
                .endMetadata()
                .build()
        );
        pods.add(new PodBuilder()
                .withNewMetadata()
                    .withName("pod-1")
                    .withNamespace(NAMESPACE)
                    .withAnnotations(annotations)
                .endMetadata()
                .build()
        );

        StrimziPodSet sps = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-sps")
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withPods(PodSetUtils.podsToMaps(pods))
                .endSpec()
                .build();

        List<Pod> resultPods = PodSetUtils.podSetToPods(WorkloadUtils.patchAnnotations(sps, Map.of("anno-2", "value-2a", "anno-4", "value-4")));
        assertThat(resultPods.size(), is(2));
        Map<String, String> expectedAnnotations = Map.of("anno-1", "value-1", "anno-2", "value-2a", "anno-3", "value-3", "anno-4", "value-4");
        assertThat(resultPods.get(0).getMetadata().getAnnotations(), is(expectedAnnotations));
        assertThat(resultPods.get(1).getMetadata().getAnnotations(), is(expectedAnnotations));
    }

    //////////////////////////////////////////////////
    // Stateful Pod tests
    //////////////////////////////////////////////////

    @Test
    public void testCreateStatefulPodWithNullValues()  {
        Pod pod = WorkloadUtils.createStatefulPod(
                Reconciliation.DUMMY_RECONCILIATION,
                NAME + "-0",    // => Pod name
                NAMESPACE,
                LABELS,
                NAME,   // => Workload name
                NAME + "-sa",   // => Service Account name
                null,
                null,
                null,
                HEADLESS_SERVICE_NAME,
                null,
                null,
                List.of(new ContainerBuilder().withName("container").build()),
                null,
                null,
                null
        );

        assertThat(pod.getMetadata().getName(), is(NAME + "-0"));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pod.getMetadata().getLabels(), is(LABELS
                .withStrimziPodSetController(NAME)
                .withStrimziPodName(NAME + "-0")
                .toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of(PodRevision.STRIMZI_REVISION_ANNOTATION, "eaf3698c")));

        assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
        assertThat(pod.getSpec().getHostname(), is(NAME + "-0"));
        assertThat(pod.getSpec().getServiceAccountName(), is(NAME + "-sa"));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(nullValue()));
        assertThat(pod.getSpec().getInitContainers(), is(nullValue()));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(nullValue()));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(nullValue()));
        assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreateStatefulPodWithNullValuesAndNullTemplate()  {
        Pod pod = WorkloadUtils.createStatefulPod(
                Reconciliation.DUMMY_RECONCILIATION,
                NAME + "-0",    // => Pod name
                NAMESPACE,
                LABELS,
                NAME,   // => Workload name
                NAME + "-sa",   // => Service Account name
                null,
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                HEADLESS_SERVICE_NAME,
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getName(), is(NAME + "-0"));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pod.getMetadata().getLabels(), is(LABELS
                .withStrimziPodSetController(NAME)
                .withStrimziPodName(NAME + "-0")
                .withAdditionalLabels(Map.of("default-label", "default-value"))
                .toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations", PodRevision.STRIMZI_REVISION_ANNOTATION, "65a2d237")));

        assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
        assertThat(pod.getSpec().getHostname(), is(NAME + "-0"));
        assertThat(pod.getSpec().getServiceAccountName(), is(NAME + "-sa"));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreateStatefulPodWithEmptyTemplate()  {
        Pod pod = WorkloadUtils.createStatefulPod(
                Reconciliation.DUMMY_RECONCILIATION,
                NAME + "-0",    // => Pod name
                NAMESPACE,
                LABELS,
                NAME,   // => Workload name
                NAME + "-sa",   // => Service Account name
                new PodTemplate(),
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                HEADLESS_SERVICE_NAME,
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getName(), is(NAME + "-0"));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pod.getMetadata().getLabels(), is(LABELS
                .withStrimziPodSetController(NAME)
                .withStrimziPodName(NAME + "-0")
                .withAdditionalLabels(Map.of("default-label", "default-value"))
                .toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations", PodRevision.STRIMZI_REVISION_ANNOTATION, "65a2d237")));

        assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
        assertThat(pod.getSpec().getHostname(), is(NAME + "-0"));
        assertThat(pod.getSpec().getServiceAccountName(), is(NAME + "-sa"));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreateStatefulPodWithTemplate()  {
        Pod pod = WorkloadUtils.createStatefulPod(
                Reconciliation.DUMMY_RECONCILIATION,
                NAME + "-0",    // => Pod name
                NAMESPACE,
                LABELS,
                NAME,   // => Workload name
                NAME + "-sa",   // => Service Account name
                new PodTemplateBuilder()
                        .withNewMetadata()
                            .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                            .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                        .endMetadata()
                        .withEnableServiceLinks(false)
                        .withAffinity(new Affinity()) // => should be ignored
                        .withImagePullSecrets(List.of(new LocalObjectReference("some-other-pull-secret")))
                        .withPriorityClassName("my-priority-class")
                        .withHostAliases(DEFAULT_HOST_ALIAS)
                        .withDnsPolicy(DEFAULT_DNS_POLICY)
                        .withDnsConfig(DEFAULT_DNS_CONFIG)
                        .withTolerations(DEFAULT_TOLERATION)
                        .withTerminationGracePeriodSeconds(15)
                        .withSecurityContext(new PodSecurityContextBuilder().withRunAsUser(0L).build()) // => should be ignored
                        .withTopologySpreadConstraints(DEFAULT_TOPOLOGY_SPREAD_CONSTRAINT)
                        .withSchedulerName("my-scheduler")
                        .build(),
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                HEADLESS_SERVICE_NAME,
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getName(), is(NAME + "-0"));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pod.getMetadata().getLabels(), is(LABELS
                .withStrimziPodSetController(NAME)
                .withStrimziPodName(NAME + "-0")
                .withAdditionalLabels(Map.of("default-label", "default-value", "label-3", "value-3", "label-4", "value-4"))
                .toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations", "anno-1", "value-1", "anno-2", "value-2", PodRevision.STRIMZI_REVISION_ANNOTATION, "6458a317")));

        assertThat(pod.getSpec().getRestartPolicy(), is("Always"));
        assertThat(pod.getSpec().getHostname(), is(NAME + "-0"));
        assertThat(pod.getSpec().getServiceAccountName(), is(NAME + "-sa"));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(List.of(DEFAULT_TOLERATION)));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(15L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-other-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is("my-priority-class"));
        assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(List.of(DEFAULT_HOST_ALIAS)));
        assertThat(pod.getSpec().getDnsPolicy(), is(DEFAULT_DNS_POLICY.toValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(DEFAULT_DNS_CONFIG));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(List.of(DEFAULT_TOPOLOGY_SPREAD_CONSTRAINT)));
    }

    //////////////////////////////////////////////////
    // PodTemplateSpec tests
    //////////////////////////////////////////////////

    @Test
    public void testCreatePodTemplateSpecWithNullValues()  {
        PodTemplateSpec pod = WorkloadUtils.createPodTemplateSpec(
                NAME,
                LABELS,
                null,
                null,
                null,
                null,
                null,
                List.of(new ContainerBuilder().withName("container").build()),
                null,
                null,
                null
        );

        assertThat(pod.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of()));

        assertThat(pod.getSpec().getServiceAccountName(), is(NAME));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(nullValue()));
        assertThat(pod.getSpec().getInitContainers(), is(nullValue()));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(nullValue()));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(nullValue()));
        assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreatePodTemplateSpecWithNullTemplate()  {
        PodTemplateSpec pod = WorkloadUtils.createPodTemplateSpec(
                NAME,
                LABELS,
                null,
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("default-label", "default-value")).toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations")));

        assertThat(pod.getSpec().getServiceAccountName(), is(NAME));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreatePodTemplateSpecWithEmptyTemplate()  {
        PodTemplateSpec pod = WorkloadUtils.createPodTemplateSpec(
                NAME,
                LABELS,
                new PodTemplate(),
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("default-label", "default-value")).toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations")));

        assertThat(pod.getSpec().getServiceAccountName(), is(NAME));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreatePodTemplateSpecWithTemplate()  {
        PodTemplateSpec pod = WorkloadUtils.createPodTemplateSpec(
                NAME,
                LABELS,
                new PodTemplateBuilder()
                        .withNewMetadata()
                            .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                            .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                        .endMetadata()
                        .withEnableServiceLinks(false)
                        .withAffinity(new Affinity()) // => should be ignored
                        .withImagePullSecrets(List.of(new LocalObjectReference("some-other-pull-secret")))
                        .withPriorityClassName("my-priority-class")
                        .withHostAliases(DEFAULT_HOST_ALIAS)
                        .withDnsPolicy(DEFAULT_DNS_POLICY)
                        .withDnsConfig(DEFAULT_DNS_CONFIG)
                        .withTolerations(DEFAULT_TOLERATION)
                        .withTerminationGracePeriodSeconds(15)
                        .withSecurityContext(new PodSecurityContextBuilder().withRunAsUser(0L).build()) // => should be ignored
                        .withTopologySpreadConstraints(DEFAULT_TOPOLOGY_SPREAD_CONSTRAINT)
                        .withSchedulerName("my-scheduler")
                        .build(),
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("default-label", "default-value", "label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations", "anno-1", "value-1", "anno-2", "value-2")));

        assertThat(pod.getSpec().getServiceAccountName(), is(NAME));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(List.of(DEFAULT_TOLERATION)));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(15L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-other-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is("my-priority-class"));
        assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(List.of(DEFAULT_HOST_ALIAS)));
        assertThat(pod.getSpec().getDnsPolicy(), is(DEFAULT_DNS_POLICY.toValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(DEFAULT_DNS_CONFIG));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(List.of(DEFAULT_TOPOLOGY_SPREAD_CONSTRAINT)));
    }

    //////////////////////////////////////////////////
    // Pod tests
    //////////////////////////////////////////////////

    @Test
    public void testCreatePodWithNullValues()  {
        Pod pod = WorkloadUtils.createPod(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                null,
                null,
                null,
                null,
                null,
                List.of(new ContainerBuilder().withName("container").build()),
                null,
                null,
                null
        );

        assertThat(pod.getMetadata().getName(), is(NAME));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pod.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of()));

        assertThat(pod.getSpec().getRestartPolicy(), is("Never"));
        assertThat(pod.getSpec().getServiceAccountName(), is(NAME));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(nullValue()));
        assertThat(pod.getSpec().getInitContainers(), is(nullValue()));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(nullValue()));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(nullValue()));
        assertThat(pod.getSpec().getSecurityContext(), is(nullValue()));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreatePodWithNullValuesAndNullTemplate()  {
        Pod pod = WorkloadUtils.createPod(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                null,
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getName(), is(NAME));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pod.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("default-label", "default-value")).toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations")));

        assertThat(pod.getSpec().getRestartPolicy(), is("Never"));
        assertThat(pod.getSpec().getServiceAccountName(), is(NAME));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreatePodWithEmptyTemplate()  {
        Pod pod = WorkloadUtils.createPod(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                new PodTemplate(),
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getName(), is(NAME));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pod.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("default-label", "default-value")).toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations")));

        assertThat(pod.getSpec().getRestartPolicy(), is("Never"));
        assertThat(pod.getSpec().getServiceAccountName(), is(NAME));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(nullValue()));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(nullValue()));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(30L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is(nullValue()));
        assertThat(pod.getSpec().getSchedulerName(), is("default-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(nullValue()));
        assertThat(pod.getSpec().getDnsPolicy(), is(nullValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(nullValue()));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(nullValue()));
    }

    @Test
    public void testCreatePodWithTemplate()  {
        Pod pod = WorkloadUtils.createPod(
                NAME,
                NAMESPACE,
                LABELS,
                ResourceUtils.DUMMY_OWNER_REFERENCE,
                new PodTemplateBuilder()
                        .withNewMetadata()
                        .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                        .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
                        .endMetadata()
                        .withEnableServiceLinks(false)
                        .withAffinity(new Affinity()) // => should be ignored
                        .withImagePullSecrets(List.of(new LocalObjectReference("some-other-pull-secret")))
                        .withPriorityClassName("my-priority-class")
                        .withHostAliases(DEFAULT_HOST_ALIAS)
                        .withDnsPolicy(DEFAULT_DNS_POLICY)
                        .withDnsConfig(DEFAULT_DNS_CONFIG)
                        .withTolerations(DEFAULT_TOLERATION)
                        .withTerminationGracePeriodSeconds(15)
                        .withSecurityContext(new PodSecurityContextBuilder().withRunAsUser(0L).build()) // => should be ignored
                        .withTopologySpreadConstraints(DEFAULT_TOPOLOGY_SPREAD_CONSTRAINT)
                        .withSchedulerName("my-scheduler")
                        .build(),
                Map.of("default-label", "default-value"),
                Map.of("extra", "annotations"),
                DEFAULT_AFFINITY,
                List.of(new ContainerBuilder().withName("init-container").build()),
                List.of(new ContainerBuilder().withName("container").build()),
                VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false),
                List.of(new LocalObjectReference("some-pull-secret")),
                DEFAULT_POD_SECURITY_CONTEXT
        );

        assertThat(pod.getMetadata().getName(), is(NAME));
        assertThat(pod.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pod.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("default-label", "default-value", "label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(pod.getMetadata().getAnnotations(), is(Map.of("extra", "annotations", "anno-1", "value-1", "anno-2", "value-2")));

        assertThat(pod.getSpec().getRestartPolicy(), is("Never"));
        assertThat(pod.getSpec().getServiceAccountName(), is(NAME));
        assertThat(pod.getSpec().getEnableServiceLinks(), is(false));
        assertThat(pod.getSpec().getAffinity(), is(DEFAULT_AFFINITY));
        assertThat(pod.getSpec().getInitContainers().size(), is(1));
        assertThat(pod.getSpec().getInitContainers().get(0).getName(), is("init-container"));
        assertThat(pod.getSpec().getContainers().size(), is(1));
        assertThat(pod.getSpec().getContainers().get(0).getName(), is("container"));
        assertThat(pod.getSpec().getVolumes(), is(VolumeUtils.createPodSetVolumes(NAME + "-0", DEFAULT_STORAGE, false)));
        assertThat(pod.getSpec().getTolerations(), is(List.of(DEFAULT_TOLERATION)));
        assertThat(pod.getSpec().getTerminationGracePeriodSeconds(), is(15L));
        assertThat(pod.getSpec().getImagePullSecrets(), is(List.of(new LocalObjectReference("some-other-pull-secret"))));
        assertThat(pod.getSpec().getSecurityContext(), is(DEFAULT_POD_SECURITY_CONTEXT));
        assertThat(pod.getSpec().getPriorityClassName(), is("my-priority-class"));
        assertThat(pod.getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(pod.getSpec().getHostAliases(), is(List.of(DEFAULT_HOST_ALIAS)));
        assertThat(pod.getSpec().getDnsPolicy(), is(DEFAULT_DNS_POLICY.toValue()));
        assertThat(pod.getSpec().getDnsConfig(), is(DEFAULT_DNS_CONFIG));
        assertThat(pod.getSpec().getTopologySpreadConstraints(), is(List.of(DEFAULT_TOPOLOGY_SPREAD_CONSTRAINT)));
    }

    //////////////////////////////////////////////////
    // Helper methods tests
    //////////////////////////////////////////////////

    @Test
    public void testImagePullSecrets()  {
        List<LocalObjectReference> defaults = List.of(new LocalObjectReferenceBuilder().withName("default").build());
        List<LocalObjectReference> custom = List.of(new LocalObjectReferenceBuilder().withName("custom").build());

        assertThat(WorkloadUtils.imagePullSecrets(null, defaults), is(defaults));
        assertThat(WorkloadUtils.imagePullSecrets(new PodTemplate(), defaults), is(defaults));
        assertThat(WorkloadUtils.imagePullSecrets(new PodTemplateBuilder().withImagePullSecrets(custom).build(), defaults), is(custom));
    }

    @Test
    public void testDeploymentStrategyRecreate()    {
        DeploymentStrategy strategy = WorkloadUtils.deploymentStrategy(io.strimzi.api.kafka.model.common.template.DeploymentStrategy.RECREATE);

        assertThat(strategy.getType(), is("Recreate"));
        assertThat(strategy.getRollingUpdate(), is(nullValue()));
    }

    @Test
    public void testDeploymentStrategyRollingUpdate()    {
        DeploymentStrategy strategy = WorkloadUtils.deploymentStrategy(io.strimzi.api.kafka.model.common.template.DeploymentStrategy.ROLLING_UPDATE);

        assertThat(strategy.getType(), is("RollingUpdate"));
        assertThat(strategy.getRollingUpdate().getMaxSurge(), is(new IntOrString(1)));
        assertThat(strategy.getRollingUpdate().getMaxUnavailable(), is(new IntOrString(0)));
    }
}
