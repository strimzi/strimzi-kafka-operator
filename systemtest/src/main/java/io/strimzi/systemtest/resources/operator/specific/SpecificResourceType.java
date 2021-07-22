/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator.specific;

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Interface for resources which has different deployment strategies such as Helm or Olm.
 */
public interface SpecificResourceType {

    /**
     * Creates specific resource
     */
    void create(ExtensionContext extensionContext);

    /**
     * Delete specific resource
     */
    void delete();
}
