/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import io.strimzi.api.kafka.model.topic.KafkaTopic;

/**
 * A {@code DefaultKubernetesResourceList<Topic>} required for using Fabric8 CRD support.
 */
public class KafkaTopicList extends DefaultKubernetesResourceList<KafkaTopic> {
    private static final long serialVersionUID = 1L;
}
