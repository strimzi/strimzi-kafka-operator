/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

/**
 * Interface used for template objects which allow to configure template for JMX Secret
 */
public interface HasJmxSecretTemplate {
    /**
     * Gets the JMX Secret template
     *
     * @return  JMX Secret template
     */
    ResourceTemplate getJmxSecret();

    /**
     * Sets the JMX Secret template
     *
     * @param metadata  JMX Secret template
     */
    void setJmxSecret(ResourceTemplate metadata);
}
