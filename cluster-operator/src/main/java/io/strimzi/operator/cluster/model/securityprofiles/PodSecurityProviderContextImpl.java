/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.plugin.security.profiles.PodSecurityProviderContext;

/**
 * Implements the context for generating the Pod security context
 */
public class PodSecurityProviderContextImpl implements PodSecurityProviderContext {
    private final Storage storage;
    private final PodSecurityContext userSuppliedSecurityContext;

    /**
     * Constructor which can be used when only the user-supplied security context is set, but no storage is used.
     * Storage will be automatically set to null.
     *
     * @param podTemplate   Pod template with user-supplied security context
     */
    public PodSecurityProviderContextImpl(PodTemplate podTemplate)   {
        this(null, podTemplate);
    }

    /**
     * Constructor for setting both user-supplied security context as well as the storage configuration.
     *
     * @param storage       Storage configuration
     * @param podTemplate   Pod template with user-supplied security context
     */
    public PodSecurityProviderContextImpl(Storage storage, PodTemplate podTemplate) {
        this.storage = storage;
        this.userSuppliedSecurityContext = podTemplate != null ? podTemplate.getSecurityContext() : null;
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
