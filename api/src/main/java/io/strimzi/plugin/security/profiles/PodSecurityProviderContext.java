/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.strimzi.api.kafka.model.storage.Storage;

public interface PodSecurityProviderContext {
    Storage storage();
    PodSecurityContext userSuppliedSecurityContext();
}
