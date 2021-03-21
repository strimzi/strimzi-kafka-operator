/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.specific;

/**
 * Interface for resources which has different deployment strategies such as Helm or Olm.
 */
public interface SpecificResourceType {

    /**
     * Creates specific resource
     */
    void create();

    /**
     * Delete specific resource
     */
    void delete();
}
