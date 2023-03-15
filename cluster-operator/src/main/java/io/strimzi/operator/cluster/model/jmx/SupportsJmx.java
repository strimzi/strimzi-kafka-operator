/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.jmx;

/**
 * This interface is used for models which implement JMX support
 */
public interface SupportsJmx {
    /**
     * @return  Jmx model
     */
    JmxModel jmx();
}
