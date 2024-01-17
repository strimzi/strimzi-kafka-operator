/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.jmx;

import io.strimzi.api.kafka.model.common.template.HasJmxSecretTemplate;

/**
 * This interface is used for sections of our custom resources JMX configuration
 */
public interface HasJmxOptions {
    /**
     * Gets the JMX options
     *
     * @return  JMX options
     */
    KafkaJmxOptions getJmxOptions();

    /**
     * Sets the JMX Options
     *
     * @param jmxOptions    JMX Options
     */
    void setJmxOptions(KafkaJmxOptions jmxOptions);

    /**
     * Gets a template which contains JMX Secret template
     *
     * @return  Template with JMX Secret template
     */
    HasJmxSecretTemplate getTemplate();
}
