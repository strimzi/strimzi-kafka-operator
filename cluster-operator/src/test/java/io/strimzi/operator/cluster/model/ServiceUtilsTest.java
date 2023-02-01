/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.strimzi.api.kafka.model.template.InternalServiceTemplate;
import io.strimzi.api.kafka.model.template.InternalServiceTemplateBuilder;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class ServiceUtilsTest {
    private final static String NAME = "my-service";
    private final static String NAMESPACE = "my-namespace";
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("my-kind")
            .withName("my-name")
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-name")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type")
            .withAdditionalLabels(Map.of("label-1", "value-1", "label-2", "value-2"));
    private static final String PORT_NAME = "my-port";
    private static final ServicePort PORT = ServiceUtils.createServicePort(PORT_NAME, 1234, 5678, "HTTP");
    private static final InternalServiceTemplate TEMPLATE = new InternalServiceTemplateBuilder()
            .withNewMetadata()
                .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
            .endMetadata()
            .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
            .withIpFamilies(IpFamily.IPV4, IpFamily.IPV6)
            .build();

    @ParallelTest
    public void testCreateServicePort() {
        ServicePort port = ServiceUtils.createServicePort(PORT_NAME, 1234, 5678, "HTTP");

        assertThat(port.getName(), is(PORT_NAME));
        assertThat(port.getPort(), is(1234));
        assertThat(port.getTargetPort().getIntVal(), is(5678));
        assertThat(port.getNodePort(), is(nullValue()));
        assertThat(port.getProtocol(), is("HTTP"));
    }

    @ParallelTest
    public void testCreateServiceWithNodePort() {
        ServicePort port = ServiceUtils.createServicePort(PORT_NAME, 1234, 5678, 30000, "HTTP");

        assertThat(port.getName(), is(PORT_NAME));
        assertThat(port.getPort(), is(1234));
        assertThat(port.getTargetPort().getIntVal(), is(5678));
        assertThat(port.getNodePort(), is(30000));
        assertThat(port.getProtocol(), is("HTTP"));
    }

    @ParallelTest
    public void testCreateHeadlessServiceWithNullTemplate() {
        Service svc = ServiceUtils.createHeadlessService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null, List.of(PORT));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getClusterIP(), is("None"));
        assertThat(svc.getSpec().getSelector().size(), is(3));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getPublishNotReadyAddresses(), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));
    }

    @ParallelTest
    public void testCreateHeadlessServiceWithEmptyTemplate() {
        Service svc = ServiceUtils.createHeadlessService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, new InternalServiceTemplate(), List.of(PORT));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getClusterIP(), is("None"));
        assertThat(svc.getSpec().getSelector().size(), is(3));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getPublishNotReadyAddresses(), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));
    }

    @ParallelTest
    public void testCreateHeadlessServiceWithTemplate() {
        Service svc = ServiceUtils.createHeadlessService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, TEMPLATE, List.of(PORT));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getClusterIP(), is("None"));
        assertThat(svc.getSpec().getSelector().size(), is(3));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getPublishNotReadyAddresses(), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), is(List.of("IPv4", "IPv6")));
    }

    @ParallelTest
    public void testCreateClusterIpServiceWithNullTemplate() {
        Service svc = ServiceUtils.createClusterIpService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null, List.of(PORT));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of()));

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getSelector().size(), is(3));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));
    }

    @ParallelTest
    public void testCreateClusterIpServiceWithEmptyTemplate() {
        Service svc = ServiceUtils.createClusterIpService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, new InternalServiceTemplate(), List.of(PORT));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of()));

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getSelector().size(), is(3));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));
    }

    @ParallelTest
    public void testCreateClusterIpServiceWithTemplate() {
        Service svc = ServiceUtils.createClusterIpService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, TEMPLATE, List.of(PORT));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getSelector().size(), is(3));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), is(List.of("IPv4", "IPv6")));
    }

    @ParallelTest
    public void testCreateDiscoverableServiceWithNullTemplate() {
        Service svc = ServiceUtils.createDiscoverableClusterIpService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null, List.of(PORT), Map.of("discovery-label", "label-value"), Map.of("strimzi.io/discovery-anno", "anno-value"));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.withStrimziDiscovery().withAdditionalLabels(Map.of("discovery-label", "label-value")).toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of("strimzi.io/discovery-anno", "anno-value")));

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getSelector().size(), is(3));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is(nullValue()));
        assertThat(svc.getSpec().getIpFamilies(), is(nullValue()));
    }

    @ParallelTest
    public void testCreateDiscoverableServiceWithTemplate() {
        Service svc = ServiceUtils.createDiscoverableClusterIpService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, TEMPLATE, List.of(PORT), Map.of("discovery-label", "label-value"), Map.of("strimzi.io/discovery-anno", "anno-value"));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.withStrimziDiscovery().withAdditionalLabels(Map.of("discovery-label", "label-value", "label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of("strimzi.io/discovery-anno", "anno-value", "anno-1", "value-1", "anno-2", "value-2")));

        assertThat(svc.getSpec().getType(), is("ClusterIP"));
        assertThat(svc.getSpec().getSelector().size(), is(3));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(svc.getSpec().getSelector().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), is(List.of("IPv4", "IPv6")));
    }

    @ParallelTest
    public void testCreateServiceWithNullTemplate() {
        Service svc = ServiceUtils.createService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null, List.of(PORT), Labels.fromMap(Map.of("selector-label", "selector-value")), "NodePort", Map.of("label", "label-value"), Map.of("anno", "anno-value"), IpFamilyPolicy.REQUIRE_DUAL_STACK, List.of(IpFamily.IPV6));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label", "label-value")).toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of("anno", "anno-value")));

        assertThat(svc.getSpec().getType(), is("NodePort"));
        assertThat(svc.getSpec().getSelector().size(), is(1));
        assertThat(svc.getSpec().getSelector().get("selector-label"), is("selector-value"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("RequireDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), is(List.of("IPv6")));
    }

    @ParallelTest
    public void testCreateServiceWithTemplate() {
        Service svc = ServiceUtils.createService(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, TEMPLATE, List.of(PORT), Labels.fromMap(Map.of("selector-label", "selector-value")), "NodePort", Map.of("label", "label-value"), Map.of("anno", "anno-value"), IpFamilyPolicy.REQUIRE_DUAL_STACK, List.of(IpFamily.IPV6));

        assertThat(svc.getMetadata().getName(), is(NAME));
        assertThat(svc.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(svc.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(svc.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label", "label-value", "label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(svc.getMetadata().getAnnotations(), is(Map.of("anno", "anno-value", "anno-1", "value-1", "anno-2", "value-2")));

        assertThat(svc.getSpec().getType(), is("NodePort"));
        assertThat(svc.getSpec().getSelector().size(), is(1));
        assertThat(svc.getSpec().getSelector().get("selector-label"), is("selector-value"));
        assertThat(svc.getSpec().getPorts().size(), is(1));
        assertThat(svc.getSpec().getPorts().get(0), is(PORT));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("RequireDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), is(List.of("IPv6")));
    }
}
