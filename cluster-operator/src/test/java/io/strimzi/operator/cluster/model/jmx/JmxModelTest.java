/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.jmx;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpecBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class JmxModelTest {
    private final static String NAME = "my-jmx-secret";
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
            .withStrimziComponentType("my-component-type");
    private static final Secret EXISTING_JMX_SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName(NAME)
            .endMetadata()
            .withData(Map.of(JmxModel.JMX_USERNAME_KEY, "username", JmxModel.JMX_PASSWORD_KEY, "password"))
            .build();

    @ParallelTest
    public void testDisabledJmx() {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder().build();
        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        assertThat(jmx.secretName(), is(NAME));
        assertThat(jmx.servicePorts(), is(List.of()));
        assertThat(jmx.containerPorts(), is(List.of()));
        assertThat(jmx.envVars(), is(List.of()));
        assertThat(jmx.networkPolicyIngresRules(), is(List.of()));
        assertThat(jmx.jmxSecret(null), is(nullValue()));
    }

    @ParallelTest
    public void testEnabledJmx() {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder()
                .withNewJmxOptions()
                .endJmxOptions()
                .build();

        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        assertThat(jmx.secretName(), is(NAME));

        assertThat(jmx.servicePorts().size(), is(1));
        assertThat(jmx.servicePorts().get(0).getName(), is("jmx"));
        assertThat(jmx.servicePorts().get(0).getPort(), is(9999));
        assertThat(jmx.servicePorts().get(0).getTargetPort().getIntVal(), is(9999));
        assertThat(jmx.servicePorts().get(0).getProtocol(), is("TCP"));

        assertThat(jmx.containerPorts().size(), is(1));
        assertThat(jmx.containerPorts().get(0).getName(), is("jmx"));
        assertThat(jmx.containerPorts().get(0).getContainerPort(), is(9999));

        assertThat(jmx.envVars().size(), is(1));
        assertThat(jmx.envVars().get(0).getName(), is(JmxModel.ENV_VAR_STRIMZI_JMX_ENABLED));
        assertThat(jmx.envVars().get(0).getValue(), is("true"));

        assertThat(jmx.networkPolicyIngresRules().size(), is(1));
        assertThat(jmx.networkPolicyIngresRules().get(0).getPorts().size(), is(1));
        assertThat(jmx.networkPolicyIngresRules().get(0).getPorts().get(0).getPort().getIntVal(), is(9999));
        assertThat(jmx.networkPolicyIngresRules().get(0).getFrom(), is(List.of()));

        assertThat(jmx.jmxSecret(null), is(nullValue()));
    }

    @ParallelTest
    public void testEnabledJmxWithAuthentication() {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder()
                .withNewJmxOptions()
                    .withNewKafkaJmxAuthenticationPassword()
                    .endKafkaJmxAuthenticationPassword()
                .endJmxOptions()
                .build();

        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        assertThat(jmx.secretName(), is(NAME));

        assertThat(jmx.servicePorts().size(), is(1));
        assertThat(jmx.servicePorts().get(0).getName(), is("jmx"));
        assertThat(jmx.servicePorts().get(0).getPort(), is(9999));
        assertThat(jmx.servicePorts().get(0).getTargetPort().getIntVal(), is(9999));
        assertThat(jmx.servicePorts().get(0).getProtocol(), is("TCP"));

        assertThat(jmx.containerPorts().size(), is(1));
        assertThat(jmx.containerPorts().get(0).getName(), is("jmx"));
        assertThat(jmx.containerPorts().get(0).getContainerPort(), is(9999));

        assertThat(jmx.envVars().size(), is(3));
        assertThat(jmx.envVars().get(0).getName(), is(JmxModel.ENV_VAR_STRIMZI_JMX_ENABLED));
        assertThat(jmx.envVars().get(0).getValue(), is("true"));
        assertThat(jmx.envVars().get(1).getName(), is(JmxModel.ENV_VAR_STRIMZI_JMX_USERNAME));
        assertThat(jmx.envVars().get(1).getValueFrom().getSecretKeyRef().getName(), is(NAME));
        assertThat(jmx.envVars().get(1).getValueFrom().getSecretKeyRef().getKey(), is(JmxModel.JMX_USERNAME_KEY));
        assertThat(jmx.envVars().get(2).getName(), is(JmxModel.ENV_VAR_STRIMZI_JMX_PASSWORD));
        assertThat(jmx.envVars().get(2).getValueFrom().getSecretKeyRef().getName(), is(NAME));
        assertThat(jmx.envVars().get(2).getValueFrom().getSecretKeyRef().getKey(), is(JmxModel.JMX_PASSWORD_KEY));

        assertThat(jmx.networkPolicyIngresRules().size(), is(1));
        assertThat(jmx.networkPolicyIngresRules().get(0).getPorts().size(), is(1));
        assertThat(jmx.networkPolicyIngresRules().get(0).getPorts().get(0).getPort().getIntVal(), is(9999));
        assertThat(jmx.networkPolicyIngresRules().get(0).getFrom(), is(List.of()));

        Secret newSecret = jmx.jmxSecret(null);
        assertThat(newSecret.getMetadata().getName(), is(NAME));
        assertThat(newSecret.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(newSecret.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(newSecret.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(newSecret.getMetadata().getAnnotations(), is(nullValue()));
        assertThat(newSecret.getData().size(), is(2));
        assertThat(newSecret.getData().get(JmxModel.JMX_USERNAME_KEY), is(notNullValue()));
        assertThat(newSecret.getData().get(JmxModel.JMX_PASSWORD_KEY), is(notNullValue()));

        Secret existingSecret = jmx.jmxSecret(EXISTING_JMX_SECRET);
        assertThat(existingSecret.getMetadata().getName(), is(NAME));
        assertThat(existingSecret.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(existingSecret.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(existingSecret.getMetadata().getLabels(), is(LABELS.toMap()));
        assertThat(existingSecret.getMetadata().getAnnotations(), is(nullValue()));
        assertThat(existingSecret.getData().size(), is(2));
        assertThat(existingSecret.getData().get(JmxModel.JMX_USERNAME_KEY), is("username"));
        assertThat(existingSecret.getData().get(JmxModel.JMX_PASSWORD_KEY), is("password"));
    }

    @ParallelTest
    public void testEnabledJmxWithAuthenticationAndTemplate() {
        KafkaClusterSpec spec = new KafkaClusterSpecBuilder()
                .withNewJmxOptions()
                    .withNewKafkaJmxAuthenticationPassword()
                    .endKafkaJmxAuthenticationPassword()
                .endJmxOptions()
                .withNewTemplate()
                    .withNewJmxSecret()
                        .withNewMetadata()
                            .withLabels(Map.of("label1", "value1"))
                            .withAnnotations(Map.of("anno1", "value1"))
                        .endMetadata()
                    .endJmxSecret()
                .endTemplate()
                .build();

        JmxModel jmx = new JmxModel(NAMESPACE, NAME, LABELS, OWNER_REFERENCE, spec);

        Secret newSecret = jmx.jmxSecret(null);
        assertThat(newSecret.getMetadata().getName(), is(NAME));
        assertThat(newSecret.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(newSecret.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(newSecret.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label1", "value1")).toMap()));
        assertThat(newSecret.getMetadata().getAnnotations(), is(Map.of("anno1", "value1")));
        assertThat(newSecret.getData().size(), is(2));
        assertThat(newSecret.getData().get(JmxModel.JMX_USERNAME_KEY), is(notNullValue()));
        assertThat(newSecret.getData().get(JmxModel.JMX_PASSWORD_KEY), is(notNullValue()));

        Secret existingSecret = jmx.jmxSecret(EXISTING_JMX_SECRET);
        assertThat(existingSecret.getMetadata().getName(), is(NAME));
        assertThat(existingSecret.getMetadata().getNamespace(), is(NAMESPACE));
        assertThat(existingSecret.getMetadata().getOwnerReferences(), is(List.of(OWNER_REFERENCE)));
        assertThat(existingSecret.getMetadata().getLabels(), is(LABELS.withAdditionalLabels(Map.of("label1", "value1")).toMap()));
        assertThat(existingSecret.getMetadata().getAnnotations(), is(Map.of("anno1", "value1")));
        assertThat(existingSecret.getData().size(), is(2));
        assertThat(existingSecret.getData().get(JmxModel.JMX_USERNAME_KEY), is("username"));
        assertThat(existingSecret.getData().get(JmxModel.JMX_PASSWORD_KEY), is("password"));
    }
}
