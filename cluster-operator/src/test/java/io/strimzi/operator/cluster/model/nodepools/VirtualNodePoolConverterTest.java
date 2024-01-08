/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.nodepools;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaClusterTemplate;
import io.strimzi.api.kafka.model.kafka.KafkaClusterTemplateBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolTemplate;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class VirtualNodePoolConverterTest {
    @Test
    public void testConvertNullTemplate()  {
        assertThat(VirtualNodePoolConverter.convertTemplate(null), is(nullValue()));
    }

    @Test
    public void testConvertTemplateWithSomeValues()  {
        KafkaClusterTemplate kafkaTemplate = new KafkaClusterTemplateBuilder()
                .withNewKafkaContainer()
                    .addToEnv(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())
                .endKafkaContainer()
                .withNewPersistentVolumeClaim()
                    .withNewMetadata()
                        .addToAnnotations(Map.of("custom-anno", "custom-anno-value"))
                    .endMetadata()
                .endPersistentVolumeClaim()
                .withNewBootstrapService()
                    .withNewMetadata()
                        .addToAnnotations(Map.of("other-custom-anno", "other-custom-anno-value"))
                    .endMetadata()
                .endBootstrapService()
                .build();

        KafkaNodePoolTemplate template = VirtualNodePoolConverter.convertTemplate(kafkaTemplate);

        assertThat(template, is(notNullValue()));
        assertThat(template.getInitContainer(), is(nullValue()));
        assertThat(template.getPodSet(), is(nullValue()));
        assertThat(template.getPod(), is(nullValue()));
        assertThat(template.getPerPodService(), is(nullValue()));
        assertThat(template.getPerPodRoute(), is(nullValue()));
        assertThat(template.getPerPodIngress(), is(nullValue()));

        assertThat(template.getKafkaContainer(), is(notNullValue()));
        assertThat(template.getKafkaContainer().getEnv().size(), is(1));
        assertThat(template.getKafkaContainer().getEnv().get(0).getName(), is("MY_ENV_VAR"));
        assertThat(template.getKafkaContainer().getEnv().get(0).getValue(), is("my-env-var-value"));

        assertThat(template.getPersistentVolumeClaim(), is(notNullValue()));
        assertThat(template.getPersistentVolumeClaim().getMetadata().getAnnotations(), is(Map.of("custom-anno", "custom-anno-value")));
    }

    @Test
    public void testConvertTemplateWithAllValues()  {
        KafkaClusterTemplate kafkaTemplate = new KafkaClusterTemplateBuilder()
                .withNewInitContainer()
                    .addToEnv(new ContainerEnvVarBuilder().withName("MY_INIT_ENV_VAR").withValue("my-init-env-var-value").build())
                .endInitContainer()
                .withNewKafkaContainer()
                    .addToEnv(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())
                .endKafkaContainer()
                .withNewPod()
                    .withTmpDirSizeLimit("100Mi")
                .endPod()
                .withNewPodSet()
                    .withNewMetadata()
                        .addToAnnotations(Map.of("custom-podset-anno", "custom-podset-anno-value"))
                    .endMetadata()
                .endPodSet()
                .withNewPerPodService()
                    .withNewMetadata()
                        .addToAnnotations(Map.of("custom-service-anno", "custom-service-anno-value"))
                    .endMetadata()
                .endPerPodService()
                .withNewPerPodIngress()
                    .withNewMetadata()
                        .addToAnnotations(Map.of("custom-ingress-anno", "custom-ingress-anno-value"))
                    .endMetadata()
                .endPerPodIngress()
                .withNewPerPodRoute()
                    .withNewMetadata()
                        .addToAnnotations(Map.of("custom-route-anno", "custom-route-anno-value"))
                    .endMetadata()
                .endPerPodRoute()
                .withNewPersistentVolumeClaim()
                    .withNewMetadata()
                        .addToAnnotations(Map.of("custom-pvc-anno", "custom-pvc-anno-value"))
                    .endMetadata()
                .endPersistentVolumeClaim()
                .build();

        KafkaNodePoolTemplate template = VirtualNodePoolConverter.convertTemplate(kafkaTemplate);

        assertThat(template, is(notNullValue()));

        assertThat(template.getInitContainer(), is(notNullValue()));
        assertThat(template.getInitContainer().getEnv().size(), is(1));
        assertThat(template.getInitContainer().getEnv().get(0).getName(), is("MY_INIT_ENV_VAR"));
        assertThat(template.getInitContainer().getEnv().get(0).getValue(), is("my-init-env-var-value"));

        assertThat(template.getPodSet(), is(notNullValue()));
        assertThat(template.getPodSet().getMetadata().getAnnotations(), is(Map.of("custom-podset-anno", "custom-podset-anno-value")));

        assertThat(template.getPod(), is(notNullValue()));
        assertThat(template.getPod().getTmpDirSizeLimit(), is("100Mi"));

        assertThat(template.getPerPodService(), is(notNullValue()));
        assertThat(template.getPerPodService().getMetadata().getAnnotations(), is(Map.of("custom-service-anno", "custom-service-anno-value")));

        assertThat(template.getPerPodRoute(), is(notNullValue()));
        assertThat(template.getPerPodRoute().getMetadata().getAnnotations(), is(Map.of("custom-route-anno", "custom-route-anno-value")));

        assertThat(template.getPerPodIngress(), is(notNullValue()));
        assertThat(template.getPerPodIngress().getMetadata().getAnnotations(), is(Map.of("custom-ingress-anno", "custom-ingress-anno-value")));

        assertThat(template.getKafkaContainer(), is(notNullValue()));
        assertThat(template.getKafkaContainer().getEnv().size(), is(1));
        assertThat(template.getKafkaContainer().getEnv().get(0).getName(), is("MY_ENV_VAR"));
        assertThat(template.getKafkaContainer().getEnv().get(0).getValue(), is("my-env-var-value"));

        assertThat(template.getPersistentVolumeClaim(), is(notNullValue()));
        assertThat(template.getPersistentVolumeClaim().getMetadata().getAnnotations(), is(Map.of("custom-pvc-anno", "custom-pvc-anno-value")));
    }

    @Test
    public void testConvertMinimalKafka()  {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                    .withLabels(Map.of("custom-label", "custom-label-value"))
                    .withAnnotations(Map.of("custom-anno", "custom-anno-value"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewJbodStorage()
                            .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                        .endJbodStorage()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool pool = VirtualNodePoolConverter.convertKafkaToVirtualNodePool(kafka, null);

        // Metadata
        assertThat(pool.getMetadata().getName(), is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pool.getMetadata().getNamespace(), is("my-namespace"));
        assertThat(pool.getMetadata().getLabels(), is(Map.of("custom-label", "custom-label-value")));
        assertThat(pool.getMetadata().getAnnotations().size(), is(0));

        // Spec
        assertThat(pool.getSpec().getReplicas(), is(3));

        JbodStorage storage = (JbodStorage) pool.getSpec().getStorage();
        assertThat(storage.getVolumes().size(), is(1));
        assertThat(storage.getVolumes().get(0).getId(), is(0));
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("100Gi"));

        assertThat(pool.getSpec().getRoles(), is(List.of(ProcessRoles.BROKER)));

        // Status
        assertThat(pool.getStatus().getNodeIds(), is(nullValue()));
        assertThat(pool.getStatus().getRoles().size(), is(1));
        assertThat(pool.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));
    }

    @Test
    public void testConvertKafkaWithExistingReplicas()  {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(5)
                        .withNewJbodStorage()
                            .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                        .endJbodStorage()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool pool = VirtualNodePoolConverter.convertKafkaToVirtualNodePool(kafka, 3);

        // Status
        assertThat(pool.getStatus().getNodeIds().size(), is(3));
        assertThat(pool.getStatus().getNodeIds(), hasItems(0, 1, 2));
        assertThat(pool.getStatus().getRoles().size(), is(1));
        assertThat(pool.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));
    }

    @Test
    public void testConvertMaximalKafka()  {
        Kafka kafka = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-cluster")
                    .withNamespace("my-namespace")
                    .withLabels(Map.of("custom-label", "custom-label-value"))
                    .withAnnotations(Map.of("custom-anno", "custom-anno-value"))
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withNewJbodStorage()
                            .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                        .endJbodStorage()
                        .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build())
                        .withNewJvmOptions()
                            .withXms("2048m")
                            .withXmx("4096m")
                        .endJvmOptions()
                        .withNewTemplate()
                            .withNewKafkaContainer()
                                .addToEnv(new ContainerEnvVarBuilder().withName("MY_ENV_VAR").withValue("my-env-var-value").build())
                            .endKafkaContainer()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool pool = VirtualNodePoolConverter.convertKafkaToVirtualNodePool(kafka, 3);

        // Metadata
        assertThat(pool.getMetadata().getName(), is(VirtualNodePoolConverter.DEFAULT_NODE_POOL_NAME));
        assertThat(pool.getMetadata().getNamespace(), is("my-namespace"));
        assertThat(pool.getMetadata().getLabels(), is(Map.of("custom-label", "custom-label-value")));
        assertThat(pool.getMetadata().getAnnotations().size(), is(0));

        // Spec
        assertThat(pool.getSpec().getReplicas(), is(3));

        JbodStorage storage = (JbodStorage) pool.getSpec().getStorage();
        assertThat(storage.getVolumes().size(), is(1));
        assertThat(storage.getVolumes().get(0).getId(), is(0));
        assertThat(((PersistentClaimStorage) storage.getVolumes().get(0)).getSize(), is("100Gi"));

        assertThat(pool.getSpec().getRoles(), is(List.of(ProcessRoles.BROKER)));

        assertThat(pool.getSpec().getResources().getRequests(), is(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))));
        assertThat(pool.getSpec().getJvmOptions().getXms(), is("2048m"));
        assertThat(pool.getSpec().getJvmOptions().getXmx(), is("4096m"));

        assertThat(pool.getSpec().getTemplate().getKafkaContainer(), is(notNullValue()));
        assertThat(pool.getSpec().getTemplate().getKafkaContainer().getEnv().size(), is(1));
        assertThat(pool.getSpec().getTemplate().getKafkaContainer().getEnv().get(0).getName(), is("MY_ENV_VAR"));
        assertThat(pool.getSpec().getTemplate().getKafkaContainer().getEnv().get(0).getValue(), is("my-env-var-value"));

        // Status
        assertThat(pool.getStatus().getNodeIds().size(), is(3));
        assertThat(pool.getStatus().getNodeIds(), hasItems(0, 1, 2));
        assertThat(pool.getStatus().getRoles().size(), is(1));
        assertThat(pool.getStatus().getRoles(), hasItems(ProcessRoles.BROKER));
    }
}
