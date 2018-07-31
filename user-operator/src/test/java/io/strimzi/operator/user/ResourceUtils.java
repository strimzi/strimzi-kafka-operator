/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.operator.common.model.Labels;

import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

public class ResourceUtils {
    public static Map labels = Collections.singletonMap("foo", "bar");
    public static String namespace = "namespace";
    public static String name = "user";

    public static KafkaUser createKafkaUser() {
        return new KafkaUserBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                                .withNamespace(namespace)
                                .withName(name)
                                .withLabels(labels)
                                .build()
                )
                .withNewSpec()
                .withAuthentication(new KafkaUserTlsClientAuthentication())
                .endSpec()
                .build();
    }

    public static Secret createClientsCa()  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName("somename")
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("clients-ca.key", Base64.getEncoder().encodeToString("clients-ca-key".getBytes()))
                .addToData("clients-ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .build();
    }

    public static Secret createUserCert()  {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(Labels.userLabels(labels).withKind(KafkaUser.RESOURCE_KIND).toMap())
                .endMetadata()
                .addToData("ca.crt", Base64.getEncoder().encodeToString("clients-ca-crt".getBytes()))
                .addToData("user.key", Base64.getEncoder().encodeToString("expected-key".getBytes()))
                .addToData("user.crt", Base64.getEncoder().encodeToString("expected-crt".getBytes()))
                .build();
    }
}
