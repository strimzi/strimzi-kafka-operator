/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Probe;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplateBuilder;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.api.kafka.model.template.PodTemplateBuilder;
import io.strimzi.operator.common.model.Labels;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.model.ModelUtils.parseImageMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ModelUtilsTest {

    @Test
    public void testParseImageMap() {
        Map<String, String> m = parseImageMap("2.0.0=strimzi/kafka:latest-kafka-2.0.0\n  " +
                "1.1.1=strimzi/kafka:latest-kafka-1.1.1");
        assertEquals(2, m.size());
        assertEquals("strimzi/kafka:latest-kafka-2.0.0", m.get("2.0.0"));
        assertEquals("strimzi/kafka:latest-kafka-1.1.1", m.get("1.1.1"));

        m = parseImageMap(" 2.0.0=strimzi/kafka:latest-kafka-2.0.0," +
                "1.1.1=strimzi/kafka:latest-kafka-1.1.1");
        assertEquals(2, m.size());
        assertEquals("strimzi/kafka:latest-kafka-2.0.0", m.get("2.0.0"));
        assertEquals("strimzi/kafka:latest-kafka-1.1.1", m.get("1.1.1"));
    }

    @Test
    public void testParsePodDisruptionBudgetTemplate()  {
        PodDisruptionBudgetTemplate template = new PodDisruptionBudgetTemplateBuilder()
                .withNewMetadata()
                .withAnnotations(Collections.singletonMap("annoKey", "annoValue"))
                .withLabels(Collections.singletonMap("labelKey", "labelValue"))
                .endMetadata()
                .withMaxUnavailable(2)
                .build();

        Model model = new Model();

        ModelUtils.parsePodDisruptionBudgetTemplate(model, template);
        assertEquals(Collections.singletonMap("labelKey", "labelValue"), model.templatePodDisruptionBudgetLabels);
        assertEquals(Collections.singletonMap("annoKey", "annoValue"), model.templatePodDisruptionBudgetAnnotations);
        assertEquals(2, model.templatePodDisruptionBudgetMaxUnavailable);
    }

    @Test
    public void testParseNullPodDisruptionBudgetTemplate()  {
        Model model = new Model();

        ModelUtils.parsePodDisruptionBudgetTemplate(model, null);
        assertNull(model.templatePodDisruptionBudgetLabels);
        assertNull(model.templatePodDisruptionBudgetAnnotations);
        assertEquals(1, model.templatePodDisruptionBudgetMaxUnavailable);
    }

    @Test
    public void testParsePodTemplate()  {
        LocalObjectReference secret1 = new LocalObjectReference("some-pull-secret");
        LocalObjectReference secret2 = new LocalObjectReference("some-other-pull-secret");

        PodTemplate template = new PodTemplateBuilder()
                .withNewMetadata()
                .withAnnotations(Collections.singletonMap("annoKey", "annoValue"))
                .withLabels(Collections.singletonMap("labelKey", "labelValue"))
                .endMetadata()
                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withRunAsUser(789L).build())
                .withImagePullSecrets(secret1, secret2)
                .withTerminationGracePeriodSeconds(123)
                .build();

        Model model = new Model();

        ModelUtils.parsePodTemplate(model, template);
        assertEquals(Collections.singletonMap("labelKey", "labelValue"), model.templatePodLabels);
        assertEquals(Collections.singletonMap("annoKey", "annoValue"), model.templatePodAnnotations);
        assertEquals(123, model.templateTerminationGracePeriodSeconds);
        assertEquals(2, model.templateImagePullSecrets.size());
        assertTrue(model.templateImagePullSecrets.contains(secret1));
        assertTrue(model.templateImagePullSecrets.contains(secret2));
        assertNotNull(model.templateSecurityContext);
        assertEquals(Long.valueOf(123), model.templateSecurityContext.getFsGroup());
        assertEquals(Long.valueOf(456), model.templateSecurityContext.getRunAsGroup());
        assertEquals(Long.valueOf(789), model.templateSecurityContext.getRunAsUser());
    }

    @Test
    public void testParseNullPodTemplate()  {
        Model model = new Model();

        ModelUtils.parsePodTemplate(model, null);
        assertNull(model.templatePodLabels);
        assertNull(model.templatePodAnnotations);
        assertNull(model.templateImagePullSecrets);
        assertNull(model.templateSecurityContext);
        assertEquals(30, model.templateTerminationGracePeriodSeconds);
    }

    private class Model extends AbstractModel   {
        public Model()  {
            super("", "", Labels.EMPTY);
        }

        @Override
        protected String getDefaultLogConfigFileName() {
            return null;
        }

        @Override
        protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
            return null;
        }
    }

    @Test
    public void testStorageSerializationAndDeserialization()    {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage ephemeral = new EphemeralStorageBuilder().build();

        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        assertEquals(jbod, ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(jbod)));
        assertEquals(ephemeral, ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(ephemeral)));
        assertEquals(persistent, ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(persistent)));
    }

    @Test
    public void testCreateTcpSocketProbe()  {
        Probe probe = ModelUtils.createTcpSocketProbe(1234, new ProbeBuilder().withInitialDelaySeconds(10).withTimeoutSeconds(20).build());
        assertEquals(new Integer(1234), probe.getTcpSocket().getPort().getIntVal());
        assertEquals(new Integer(10), probe.getInitialDelaySeconds());
        assertEquals(new Integer(20), probe.getTimeoutSeconds());
    }
}
