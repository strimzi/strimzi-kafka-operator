/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplateBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class PodDisruptionBudgetUtilsTest {
    private final static String NAME = "my-pdb";
    private final static String NAMESPACE = "my-namespace";
    private final static int REPLICAS = 5;
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
    private final static PodDisruptionBudgetTemplate TEMPLATE = new PodDisruptionBudgetTemplateBuilder()
            .withNewMetadata()
                .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
            .endMetadata()
            .withMaxUnavailable(2)
            .build();

    @ParallelTest
    public void testPdbWithoutTemplate() {
        PodDisruptionBudget pdb = PodDisruptionBudgetUtils.createPodDisruptionBudget(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null);

        assertThat(pdb.getMetadata().getName(), is(NAME));
        assertThat(pdb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pdb.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pdb.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pdb.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
        assertThat(pdb.getSpec().getMinAvailable(), is(nullValue()));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    @ParallelTest
    public void testPdbWithTemplate() {
        PodDisruptionBudget pdb = PodDisruptionBudgetUtils.createPodDisruptionBudget(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, TEMPLATE);

        assertThat(pdb.getMetadata().getName(), is(NAME));
        assertThat(pdb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pdb.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pdb.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(pdb.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
        assertThat(pdb.getSpec().getMinAvailable(), is(nullValue()));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    @ParallelTest
    public void testCustomControllerPdbWithoutTemplate() {
        PodDisruptionBudget pdb = PodDisruptionBudgetUtils.createCustomControllerPodDisruptionBudget(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null, REPLICAS);

        assertThat(pdb.getMetadata().getName(), is(NAME));
        assertThat(pdb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pdb.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pdb.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pdb.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(4)));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    @ParallelTest
    public void testCustomControllerPdbWithTemplate() {
        PodDisruptionBudget pdb = PodDisruptionBudgetUtils.createCustomControllerPodDisruptionBudget(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, TEMPLATE, REPLICAS);

        assertThat(pdb.getMetadata().getName(), is(NAME));
        assertThat(pdb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pdb.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pdb.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(pdb.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(3)));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    @ParallelTest
    public void testV1Beta1PdbWithoutTemplate() {
        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdb = PodDisruptionBudgetUtils.createPodDisruptionBudgetV1Beta1(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null);

        assertThat(pdb.getMetadata().getName(), is(NAME));
        assertThat(pdb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pdb.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pdb.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pdb.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(1)));
        assertThat(pdb.getSpec().getMinAvailable(), is(nullValue()));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    @ParallelTest
    public void testV1Beta1PdbWithTemplate() {
        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdb = PodDisruptionBudgetUtils.createPodDisruptionBudgetV1Beta1(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, TEMPLATE);

        assertThat(pdb.getMetadata().getName(), is(NAME));
        assertThat(pdb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pdb.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pdb.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(pdb.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(pdb.getSpec().getMaxUnavailable(), is(new IntOrString(2)));
        assertThat(pdb.getSpec().getMinAvailable(), is(nullValue()));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    @ParallelTest
    public void testV1Beta1CustomControllerPdbWithoutTemplate() {
        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdb = PodDisruptionBudgetUtils.createCustomControllerPodDisruptionBudgetV1Beta1(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, null, REPLICAS);

        assertThat(pdb.getMetadata().getName(), is(NAME));
        assertThat(pdb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pdb.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pdb.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pdb.getMetadata().getAnnotations(), is(nullValue()));

        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(4)));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }

    @ParallelTest
    public void testV1Beta1CustomControllerPdbWithTemplate() {
        io.fabric8.kubernetes.api.model.policy.v1beta1.PodDisruptionBudget pdb = PodDisruptionBudgetUtils.createCustomControllerPodDisruptionBudgetV1Beta1(NAME, NAMESPACE, LABELS, OWNER_REFERENCE, TEMPLATE, REPLICAS);

        assertThat(pdb.getMetadata().getName(), is(NAME));
        assertThat(pdb.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pdb.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pdb.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(pdb.getMetadata().getAnnotations(), is(Map.of("anno-1", "value-1", "anno-2", "value-2")));

        assertThat(pdb.getSpec().getMaxUnavailable(), is(nullValue()));
        assertThat(pdb.getSpec().getMinAvailable(), is(new IntOrString(3)));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().size(), is(3));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_CLUSTER_LABEL), is("my-cluster"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_NAME_LABEL), is("my-name"));
        assertThat(pdb.getSpec().getSelector().getMatchLabels().get(Labels.STRIMZI_KIND_LABEL), is("my-kind"));
    }
}
