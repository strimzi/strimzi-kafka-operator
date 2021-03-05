/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.strimzi.api.kafka.model.KafkaTopic;

public class KafkaTopicConverter extends AbstractVersionableConverter<KafkaTopic> {
    @Override
    public Class<KafkaTopic> crClass() {
        return KafkaTopic.class;
    }
}
