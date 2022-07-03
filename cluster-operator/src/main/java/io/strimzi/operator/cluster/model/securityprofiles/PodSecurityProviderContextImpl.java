/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.plugin.security.profiles.PodSecurityProviderContext;

public class PodSecurityProviderContextImpl implements PodSecurityProviderContext {
    private final Storage storage;
    private final PodSecurityContext userSuppliedSecurityContext;

    public PodSecurityProviderContextImpl(PodSecurityContext userSuppliedSecurityContext)   {
        this(null, userSuppliedSecurityContext);
    }

    public PodSecurityProviderContextImpl(Storage storage, PodSecurityContext userSuppliedSecurityContext) {
        this.storage = storage;
        this.userSuppliedSecurityContext = userSuppliedSecurityContext;
    }

    @Override
    public Storage storage() {
        return storage;
    }

    @Override
    public PodSecurityContext userSuppliedSecurityContext() {
        return userSuppliedSecurityContext;
    }
}
