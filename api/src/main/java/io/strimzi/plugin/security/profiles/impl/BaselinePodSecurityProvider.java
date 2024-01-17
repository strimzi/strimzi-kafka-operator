/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles.impl;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.platform.PlatformFeatures;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;
import io.strimzi.plugin.security.profiles.PodSecurityProviderContext;

/**
 * The default implementation of the PodSecurityProvider. It implements the Baseline Kubernetes security profile.
 */
public class BaselinePodSecurityProvider implements PodSecurityProvider {
    protected static final Long DEFAULT_FS_GROUP_ID = 0L;

    protected boolean isOpenShift = false;

    @Override
    public void configure(PlatformFeatures platformFeatures) {
        isOpenShift = platformFeatures.isOpenshift();
    }

    /**
     * Internal method which checks whether persistent storage (either Persistent Claim storage or a JBOD storage with
     * at least one Persistent Claim disk) is used or not.
     *
     * @param storage The storage configuration of the Pod / container
     *
     * @return  Returns true if persistent storage is used. Returns false otherwise.
     */
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

    /**
     * Internal method whichcreates a Pod security context for Pods using persistent storage (Kafka and ZooKeeper). If
     * any user-supplied pod security context is set, it will be used. Otherwise:
     *   - if running on OpenShift, no context will be set as OpenShift injects its own context
     *   - if running outside of OpenShift, the fsGroup will be set to the group ID 0 which is the default in the containers
     *
     * @param context   Context for providing the Pod security context
     *
     * @return  Returns the generated Pod security context
     */
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
