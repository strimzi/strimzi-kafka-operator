/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.JbodStorage;
import io.strimzi.api.kafka.model.JbodStorageBuilder;
import io.strimzi.api.kafka.model.PersistentClaimStorage;
import io.strimzi.api.kafka.model.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.operator.common.model.Labels;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StatefulClusterTest {

    public static final String MOUNT_PATH = "/var/lib/test/";

    private StatefulCluster createClusterWithStorage(Storage storage) {
        StatefulCluster sc = new StatefulCluster("test-namespace",  "test-cluster",  Labels.EMPTY) {
            {
                mountPath = "/var/lib/test";
            }

            @Override
            protected String getDefaultLogConfigFileName() {
                return null;
            }

            @Override
            protected List<Container> getContainers(ImagePullPolicy imagePullPolicy) {
                return null;
            }
        };
        sc.setStorage(storage);
        return sc;
    }

    private PersistentClaimStorage createPcs(boolean delete, Integer id) {
        PersistentClaimStorageBuilder builder = new PersistentClaimStorageBuilder();
        builder.withDeleteClaim(delete).withSize("100Gi");
        if (id != null) {
            builder.withId(id).withSubPath(id.toString());
        }
        return builder.build();
    }

    private Storage createEphemeral(Integer id) {
        return new EphemeralStorageBuilder().withId(id).build();
    }

    private JbodStorage createJbodVolume() {
        return new JbodStorageBuilder().withVolumes(
                createPcs(false, 0), createPcs(true, 1)).build();
    }

    @Test
    public void testSingleEphemeralName() {
        StatefulCluster cluster = createClusterWithStorage(createEphemeral(null));

        List<PersistentVolumeClaim> pvcs = cluster.getVolumeClaims();
        Assert.assertEquals(0, pvcs.size());
        for (VolumeMount vm : cluster.getVolumeMounts()) {
            String name = vm.getName();
            assertEquals(cluster.VOLUME_NAME, name);
            assertEquals(MOUNT_PATH + name, vm.getMountPath());
            assertNull(vm.getSubPath());
        }
    }

    @Test
    public void testSinglePvcNameNoSubpath() {
        StatefulCluster cluster = createClusterWithStorage(createPcs(false, null));

        List<PersistentVolumeClaim> pvcs = cluster.getVolumeClaims();
        Assert.assertEquals(1, pvcs.size());
        List<VolumeMount> vms = cluster.getVolumeMounts();

        String name = pvcs.get(0).getMetadata().getName();
        assertEquals(cluster.VOLUME_NAME, name);

        VolumeMount vm = vms.get(0);
        assertEquals(name, vm.getName());
        assertEquals(MOUNT_PATH + name, vm.getMountPath());
        assertNull(vm.getSubPath());
    }

    @Test
    public void testMultiplePvcNamesWithSubPath() {
        StatefulCluster cluster = createClusterWithStorage(createJbodVolume());

        List<PersistentVolumeClaim> pvcs = cluster.getVolumeClaims();
        Assert.assertEquals(2, pvcs.size());
        List<VolumeMount> vms = cluster.getVolumeMounts();

        for (int i = 0; i < pvcs.size(); ++i) {
            String name = pvcs.get(i).getMetadata().getName();
            assertEquals(cluster.VOLUME_NAME + "-" + i, name);

            VolumeMount vm = vms.get(i);
            assertEquals(name, vm.getName());
            assertEquals(MOUNT_PATH + name, vm.getMountPath());
            assertEquals(Integer.toString(i), vm.getSubPath());
        }
    }

}