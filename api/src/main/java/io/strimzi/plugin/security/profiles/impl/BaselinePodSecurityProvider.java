/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles.impl;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.platform.PlatformFeatures;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;
import io.strimzi.plugin.security.profiles.PodSecurityProviderContext;

public class BaselinePodSecurityProvider implements PodSecurityProvider {
    protected static final Long DEFAULT_FS_GROUP_ID = 0L;

    protected boolean isOpenShift = false;

    @Override
    public void configure(PlatformFeatures platformFeatures) {
        isOpenShift = platformFeatures.isOpenshift();
    }

    private boolean usesPersistentStorage(Storage storage) {
        if (storage instanceof JbodStorage)  {
            JbodStorage jbodStorage = (JbodStorage) storage;

            for (Storage jbodVolume : jbodStorage.getVolumes()) {
                if (jbodVolume instanceof PersistentClaimStorage)   {
                    return true;
                }
            }

            return false;
        } else {
            return storage instanceof PersistentClaimStorage;
        }

    }

    private PodSecurityContext createStatefulPodSecurityContext(PodSecurityProviderContext context)  {
        if (context == null)    {
            return null;
        } else if (context.userSuppliedSecurityContext() != null)    {
            return context.userSuppliedSecurityContext();
        } else if (isOpenShift)    {
            return null;
        } else if (usesPersistentStorage(context.storage())) {
            return new PodSecurityContextBuilder()
                    .withFsGroup(DEFAULT_FS_GROUP_ID)
                    .build();
        } else {
            return null;
        }
    }

    @Override
    public PodSecurityContext zooKeeperPodSecurityContext(PodSecurityProviderContext context) {
        return createStatefulPodSecurityContext(context);
    }

    @Override
    public PodSecurityContext kafkaPodSecurityContext(PodSecurityProviderContext context) {
        return createStatefulPodSecurityContext(context);
    }
}
