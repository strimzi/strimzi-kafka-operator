/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.kafka.model.KafkaUser;

public class KafkaUserConverter extends AbstractVersionableConverter<KafkaUser> {
    @Override
    public Class<KafkaUser> crClass() {
        return KafkaUser.class;
    }
}
