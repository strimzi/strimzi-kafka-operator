/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.fabric8.kubernetes.api.model.SecurityContext;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.plugin.security.profiles.ContainerSecurityProviderContext;

public class ContainerSecurityProviderContextImpl implements ContainerSecurityProviderContext {
    private final Storage storage;
    private final SecurityContext userSuppliedSecurityContext;

    public ContainerSecurityProviderContextImpl(SecurityContext userSuppliedSecurityContext)   {
        this(null, userSuppliedSecurityContext);
    }

    public ContainerSecurityProviderContextImpl(Storage storage, SecurityContext userSuppliedSecurityContext) {
        this.storage = storage;
        this.userSuppliedSecurityContext = userSuppliedSecurityContext;
    }

    @Override
    public Storage storage() {
        return storage;
    }

    @Override
    public SecurityContext userSuppliedSecurityContext() {
        return userSuppliedSecurityContext;
    }
}
