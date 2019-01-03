/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplate;
import io.strimzi.api.kafka.model.template.PodDisruptionBudgetTemplateBuilder;
import io.strimzi.api.kafka.model.template.PodTemplate;
import io.strimzi.api.kafka.model.template.PodTemplateBuilder;
import io.strimzi.operator.common.model.Labels;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FromCrdUtilsTest {
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

        FromCrdUtils.parsePodDisruptionBudgetTemplate(model, template);
        assertEquals(Collections.singletonMap("labelKey", "labelValue"), model.templatePodDisruptionBudgetLabels);
        assertEquals(Collections.singletonMap("annoKey", "annoValue"), model.templatePodDisruptionBudgetAnnotations);
        assertEquals(2, model.templatePodDisruptionBudgetMaxUnavailable);
    }

    @Test
    public void testParseNullPodDisruptionBudgetTemplate()  {
        Model model = new Model();

        FromCrdUtils.parsePodDisruptionBudgetTemplate(model, null);
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
                .withSecurityContext(new PodSecurityContextBuilder().withFsGroup(123L).withRunAsGroup(456L).withNewRunAsUser(789L).build())
                .withImagePullSecrets(secret1, secret2)
                .withTerminationGracePeriodSeconds(123)
                .build();

        Model model = new Model();

        FromCrdUtils.parsePodTemplate(model, template);
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

        FromCrdUtils.parsePodTemplate(model, null);
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
        protected List<Container> getContainers() {
            return null;
        }
    }
}


