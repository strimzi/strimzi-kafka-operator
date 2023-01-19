/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverrideBuilder;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.api.kafka.model.template.ResourceTemplateBuilder;
import io.strimzi.api.kafka.model.template.StatefulSetTemplate;
import io.strimzi.api.kafka.model.template.StatefulSetTemplateBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class PersistentVolumeClaimUtilsTest {
    private final static String NAME = "my-cluster-kafka";
    private final static String NAMESPACE = "my-namespace";
    private static final OwnerReference OWNER_REFERENCE = new OwnerReferenceBuilder()
            .withApiVersion("v1")
            .withKind("my-kind")
            .withName("my-cluster")
            .withUid("my-uid")
            .withBlockOwnerDeletion(false)
            .withController(false)
            .build();
    private static final Labels LABELS = Labels
            .forStrimziKind("my-kind")
            .withStrimziName("my-cluster-kafka")
            .withStrimziCluster("my-cluster")
            .withStrimziComponentType("my-component-type")
            .withAdditionalLabels(Map.of("label-1", "value-1", "label-2", "value-2"));
    private final static ResourceTemplate TEMPLATE = new ResourceTemplateBuilder()
            .withNewMetadata()
                .withLabels(Map.of("label-3", "value-3", "label-4", "value-4"))
                .withAnnotations(Map.of("anno-1", "value-1", "anno-2", "value-2"))
            .endMetadata()
            .build();
    private final static PersistentClaimStorage PERSISTENT_CLAIM_STORAGE = new PersistentClaimStorageBuilder()
            .withStorageClass("my-storage-class")
            .withSize("100Gi")
            .build();

    @ParallelTest
    public void testEphemeralStorage()  {
        assertThat(
                PersistentVolumeClaimUtils
                        .createPersistentVolumeClaims(NAME, NAMESPACE, 3, new EphemeralStorage(), false, LABELS, OWNER_REFERENCE, null, null),
                is(List.of())
        );
    }

    @ParallelTest
    public void testEphemeralJbodStorage()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(new EphemeralStorage(), new EphemeralStorage())
                .build();

        assertThat(
                PersistentVolumeClaimUtils
                        .createPersistentVolumeClaims(NAME, NAMESPACE, 3, jbod, false, LABELS, OWNER_REFERENCE, null, null),
                is(List.of())
        );
    }

    @ParallelTest
    public void testPersistentClaimStorage()  {
        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 1, PERSISTENT_CLAIM_STORAGE, false, LABELS, OWNER_REFERENCE, null, null);

        assertThat(pvcs.size(), is(1));

        assertThat(pvcs.get(0).getMetadata().getName(), is("data-my-cluster-kafka-0"));
        assertThat(pvcs.get(0).getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pvcs.get(0).getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pvcs.get(0).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "false")));
        assertThat(pvcs.get(0).getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(pvcs.get(0).getSpec().getVolumeMode(), is("Filesystem"));
        assertThat(pvcs.get(0).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
        assertThat(pvcs.get(0).getSpec().getSelector(), is(nullValue()));
        assertThat(pvcs.get(0).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
        assertThat(pvcs.get(0).getSpec().getStorageClassName(), is("my-storage-class"));
    }

    @ParallelTest
    public void testJbodStorage()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withStorageClass("my-storage-class")
                        .withSize("100Gi")
                        .build())
                .build();

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 1, jbod, false, LABELS, OWNER_REFERENCE, null, null);

        assertThat(pvcs.size(), is(1));

        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(0).getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pvcs.get(0).getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pvcs.get(0).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "false")));
        assertThat(pvcs.get(0).getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(pvcs.get(0).getSpec().getVolumeMode(), is("Filesystem"));
        assertThat(pvcs.get(0).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
        assertThat(pvcs.get(0).getSpec().getSelector(), is(nullValue()));
        assertThat(pvcs.get(0).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
        assertThat(pvcs.get(0).getSpec().getStorageClassName(), is("my-storage-class"));
    }

    @ParallelTest
    public void testTemplate()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withStorageClass("my-storage-class")
                        .withSize("100Gi")
                        .build())
                .build();

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 1, jbod, false, LABELS, OWNER_REFERENCE, TEMPLATE, null);

        assertThat(pvcs.size(), is(1));

        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(0).getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pvcs.get(0).getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4")).toMap()));
        assertThat(pvcs.get(0).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "false", "anno-1", "value-1", "anno-2", "value-2")));
        assertThat(pvcs.get(0).getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(pvcs.get(0).getSpec().getVolumeMode(), is("Filesystem"));
        assertThat(pvcs.get(0).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
        assertThat(pvcs.get(0).getSpec().getSelector(), is(nullValue()));
        assertThat(pvcs.get(0).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
        assertThat(pvcs.get(0).getSpec().getStorageClassName(), is("my-storage-class"));
    }

    @ParallelTest
    public void testTemplateAndStatefulSetLabels()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withStorageClass("my-storage-class")
                        .withSize("100Gi")
                        .build())
                .build();

        StatefulSetTemplate stsTemplate = new StatefulSetTemplateBuilder()
                .withNewMetadata()
                    .withLabels(Map.of("sts-labela-1", "value-from-sts-1"))
                .endMetadata()
                .build();

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 1, jbod, false, LABELS, OWNER_REFERENCE, TEMPLATE, stsTemplate);

        assertThat(pvcs.size(), is(1));

        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(0).getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pvcs.get(0).getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label-3", "value-3", "label-4", "value-4", "sts-labela-1", "value-from-sts-1")).toMap()));
        assertThat(pvcs.get(0).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "false", "anno-1", "value-1", "anno-2", "value-2")));
        assertThat(pvcs.get(0).getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(pvcs.get(0).getSpec().getVolumeMode(), is("Filesystem"));
        assertThat(pvcs.get(0).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
        assertThat(pvcs.get(0).getSpec().getSelector(), is(nullValue()));
        assertThat(pvcs.get(0).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
        assertThat(pvcs.get(0).getSpec().getStorageClassName(), is("my-storage-class"));
    }

    @ParallelTest
    public void testWithoutTemplateButWithStatefulSetLabels()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withStorageClass("my-storage-class")
                        .withSize("100Gi")
                        .build())
                .build();

        StatefulSetTemplate stsTemplate = new StatefulSetTemplateBuilder()
                .withNewMetadata()
                    .withLabels(Map.of("sts-labela-1", "value-from-sts-1"))
                .endMetadata()
                .build();

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 1, jbod, false, LABELS, OWNER_REFERENCE, null, stsTemplate);

        assertThat(pvcs.size(), is(1));

        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(0).getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pvcs.get(0).getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("sts-labela-1", "value-from-sts-1")).toMap()));
        assertThat(pvcs.get(0).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "false")));
        assertThat(pvcs.get(0).getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(pvcs.get(0).getSpec().getVolumeMode(), is("Filesystem"));
        assertThat(pvcs.get(0).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
        assertThat(pvcs.get(0).getSpec().getSelector(), is(nullValue()));
        assertThat(pvcs.get(0).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
        assertThat(pvcs.get(0).getSpec().getStorageClassName(), is("my-storage-class"));
    }

    @ParallelTest
    public void testWithSelector()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withStorageClass("my-storage-class")
                        .withSize("100Gi")
                        .withSelector(Map.of("pv-label", "pv-value"))
                        .build())
                .build();

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 1, jbod, false, LABELS, OWNER_REFERENCE, null, null);

        assertThat(pvcs.size(), is(1));

        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(0).getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pvcs.get(0).getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pvcs.get(0).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "false")));
        assertThat(pvcs.get(0).getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(pvcs.get(0).getSpec().getVolumeMode(), is("Filesystem"));
        assertThat(pvcs.get(0).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
        assertThat(pvcs.get(0).getSpec().getSelector().getMatchLabels(), is(Map.of("pv-label", "pv-value")));
        assertThat(pvcs.get(0).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
        assertThat(pvcs.get(0).getSpec().getStorageClassName(), is("my-storage-class"));
    }

    @ParallelTest
    public void testJbodStorageWithDeleteClaim()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withStorageClass("my-storage-class")
                        .withSize("100Gi")
                        .withDeleteClaim(true)
                        .build())
                .build();

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 1, jbod, false, LABELS, OWNER_REFERENCE, null, null);

        assertThat(pvcs.size(), is(1));

        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(0).getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pvcs.get(0).getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pvcs.get(0).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "true")));
        assertThat(pvcs.get(0).getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(pvcs.get(0).getSpec().getVolumeMode(), is("Filesystem"));
        assertThat(pvcs.get(0).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
        assertThat(pvcs.get(0).getSpec().getSelector(), is(nullValue()));
        assertThat(pvcs.get(0).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
        assertThat(pvcs.get(0).getSpec().getStorageClassName(), is("my-storage-class"));
    }

    @ParallelTest
    public void testWithStorageClassOverrides()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withStorageClass("my-storage-class")
                        .withSize("100Gi")
                        .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(0).withStorageClass("special-storage-class").build())
                        .build())
                .build();

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 1, jbod, false, LABELS, OWNER_REFERENCE, null, null);

        assertThat(pvcs.size(), is(1));

        assertThat(pvcs.get(0).getMetadata().getName(), is("data-0-my-cluster-kafka-0"));
        assertThat(pvcs.get(0).getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(pvcs.get(0).getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(pvcs.get(0).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "false")));
        assertThat(pvcs.get(0).getMetadata().getOwnerReferences(), is(List.of()));
        assertThat(pvcs.get(0).getSpec().getVolumeMode(), is("Filesystem"));
        assertThat(pvcs.get(0).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
        assertThat(pvcs.get(0).getSpec().getSelector(), is(nullValue()));
        assertThat(pvcs.get(0).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
        assertThat(pvcs.get(0).getSpec().getStorageClassName(), is("special-storage-class"));
    }

    @ParallelTest
    public void testJbodWithClassOverridesAndDeleteClaims()  {
        JbodStorage jbod = new JbodStorageBuilder()
                .withVolumes(
                        new PersistentClaimStorageBuilder()
                                .withId(0)
                                .withStorageClass("my-storage-class")
                                .withSize("100Gi")
                                .withDeleteClaim(false)
                                .withOverrides(new PersistentClaimStorageOverrideBuilder().withBroker(0).withStorageClass("special-storage-class").build())
                                .build(),
                        new PersistentClaimStorageBuilder()
                                .withId(1)
                                .withStorageClass("my-storage-class2")
                                .withSize("200Gi")
                                .withDeleteClaim(true)
                                .build(),
                        new EphemeralStorage())
                .build();

        List<PersistentVolumeClaim> pvcs = PersistentVolumeClaimUtils
                .createPersistentVolumeClaims(NAME, NAMESPACE, 3, jbod, false, LABELS, OWNER_REFERENCE, null, null);

        assertThat(pvcs.size(), is(6));

        for (int i = 0; i < 3; i++)  {
            assertThat(pvcs.get(i).getMetadata().getName(), is("data-0-my-cluster-kafka-" + i));
            assertThat(pvcs.get(i).getMetadata().getNamespace(), is(NAMESPACE));
            assertThat(pvcs.get(i).getMetadata().getLabels(), is(LABELS.toMap()));
            assertThat(pvcs.get(i).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "false")));
            assertThat(pvcs.get(i).getMetadata().getOwnerReferences(), is(List.of()));
            assertThat(pvcs.get(i).getSpec().getVolumeMode(), is("Filesystem"));
            assertThat(pvcs.get(i).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
            assertThat(pvcs.get(i).getSpec().getSelector(), is(nullValue()));
            assertThat(pvcs.get(i).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("100Gi", null))));
            assertThat(pvcs.get(i).getSpec().getStorageClassName(), is(i == 0 ? "special-storage-class" : "my-storage-class"));
        }

        for (int i = 3; i < 6; i++)  {
            assertThat(pvcs.get(i).getMetadata().getName(), is("data-1-my-cluster-kafka-" + i % 3));
            assertThat(pvcs.get(i).getMetadata().getNamespace(), is(NAMESPACE));
            assertThat(pvcs.get(i).getMetadata().getLabels(), is(LABELS.toMap()));
            assertThat(pvcs.get(i).getMetadata().getAnnotations(), is(Map.of("strimzi.io/delete-claim", "true")));
            assertThat(pvcs.get(i).getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
            assertThat(pvcs.get(i).getSpec().getVolumeMode(), is("Filesystem"));
            assertThat(pvcs.get(i).getSpec().getAccessModes(), is(List.of("ReadWriteOnce")));
            assertThat(pvcs.get(i).getSpec().getSelector(), is(nullValue()));
            assertThat(pvcs.get(i).getSpec().getResources().getRequests(), is(Map.of("storage", new Quantity("200Gi", null))));
            assertThat(pvcs.get(i).getSpec().getStorageClassName(), is("my-storage-class2"));
        }
    }
}
