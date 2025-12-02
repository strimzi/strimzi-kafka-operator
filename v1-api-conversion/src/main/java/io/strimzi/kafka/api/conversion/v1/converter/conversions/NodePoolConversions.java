/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;

/**
 * Class for holding the various conversions specific for the KafkaNodePool API conversion
 */
@SuppressWarnings("deprecation")
public class NodePoolConversions {
    /**
     * Removes the storage overrides in KafkaNodePool
     *
     * @return  The conversion
     */
    public static Conversion<KafkaNodePool> storageOverrides() {
        return Conversion.replace("/spec/storage", new Conversion.DefaultConversionFunction<Storage>() {
            @Override
            Class<Storage> convertedType() {
                return Storage.class;
            }

            @Override
            public Storage apply(Storage storage) {
                if (storage == null) {
                    return null;
                } else if (storage instanceof JbodStorage jbod) {
                    for (SingleVolumeStorage volume : jbod.getVolumes())   {
                        if (volume instanceof PersistentClaimStorage persistentVolume && persistentVolume.getOverrides() != null)    {
                            persistentVolume.setOverrides(null);
                        }
                    }
                } else if (storage instanceof PersistentClaimStorage persistentVolume && persistentVolume.getOverrides() != null) {
                    persistentVolume.setOverrides(null);
                }

                return storage;
            }
        });
    }
}
