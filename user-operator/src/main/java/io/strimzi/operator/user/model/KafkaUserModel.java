/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserAuthentication;

import java.util.Map;

public class KafkaUserModel {
    private final String namespace;
    private final String name;
    private final Labels labels;

    private KafkaUserAuthentication authentication;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param name   User name
     * @param labels   Labels
     */
    protected KafkaUserModel(String namespace, String name, Labels labels) {
        this.namespace = namespace;
        this.name = name;
        this.labels = labels;
    }

    public static KafkaUserModel fromCrd(KafkaUser kafkaUser) {
        KafkaUserModel result = new KafkaUserModel(kafkaUser.getMetadata().getNamespace(),
                kafkaUser.getMetadata().getName(),
                Labels.fromResource(kafkaUser).withKind(kafkaUser.getKind()));
        result.setAuthentication(kafkaUser.getSpec().getAuthentication());
        return result;
    }

    public static String getSecretName(String username)    {
        return username;
    }

    public String getSecretName()    {
        return KafkaUserModel.getSecretName(name);
    }

    public void setAuthentication(KafkaUserAuthentication authentication) {
        this.authentication = authentication;
    }
}
