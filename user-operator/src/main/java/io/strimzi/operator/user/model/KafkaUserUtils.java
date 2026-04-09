/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.strimzi.api.kafka.model.user.KafkaUser;

/**
 * Various utility methods used by the KafkaUserModel to make leaner and easier to read
 */
public class KafkaUserUtils {
    private KafkaUserUtils() { }

    /**
     * Get the user name from a {@link KafkaUser} resource.
     *
     * @param kafkaUser User resource.
     * @return User name.
     */
    public static String userName(KafkaUser kafkaUser) {
        String userName = null;
        if (kafkaUser.getSpec() != null) {
            userName = kafkaUser.getSpec().getUserName();
        }
        if (userName == null) {
            userName = kafkaUser.getMetadata().getName();
        }
        return userName;
    }
}
