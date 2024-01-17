/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.strimzi.api.kafka.model.kafka.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.Assertions;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class VolumeUtilsTest {
    private static final Storage JBOD_STORAGE = new JbodStorageBuilder().withVolumes(
                    new PersistentClaimStorageBuilder()
                        .withDeleteClaim(false)
                        .withId(0)
                        .withSize("20Gi")
                        .build(),
                    new PersistentClaimStorageBuilder()
                        .withDeleteClaim(true)
                        .withId(1)
                        .withSize("10Gi")
                        .build())
            .build();

    private static final Storage JBOD_STORAGE_WITH_EPHEMERAL = new JbodStorageBuilder().withVolumes(
                    new PersistentClaimStorageBuilder()
                        .withDeleteClaim(false)
                        .withId(0)
                        .withSize("20Gi")
                        .build(),
                    new EphemeralStorageBuilder()
                        .withId(1)
                        .build())
            .build();

    private static final Storage JBOD_STORAGE_WITHOUT_ID = new JbodStorageBuilder().withVolumes(
                    new PersistentClaimStorageBuilder()
                        .withDeleteClaim(false)
                        .withSize("20Gi")
                        .build())
            .build();

    private static final Storage PERSISTENT_CLAIM_STORAGE = new PersistentClaimStorageBuilder()
            .withDeleteClaim(false)
            .withSize("20Gi")
            .build();

    private static final Storage PERSISTENT_CLAIM_STORAGE_WITH_ID = new PersistentClaimStorageBuilder()
            .withDeleteClaim(false)
            .withId(0)
            .withSize("20Gi")
            .build();

    private static final Storage EPHEMERAL_STORAGE = new EphemeralStorageBuilder().build();


    @ParallelTest
    public void testCreateEmptyDirVolumeWithMedium() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", "1Gi", "Memory");
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));
    }

    @ParallelTest
    public void testCreateEmptyDirVolumeWithNullMedium() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", null, null);
        assertThat(volume.getEmptyDir().getMedium(), is(nullValue()));
    }

    @ParallelTest
    public void testCreateEmptyDirVolumeWithSizeLimit() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", "1Gi", null);
        assertThat(volume.getEmptyDir().getSizeLimit(), is(new Quantity("1", "Gi")));
    }

    @ParallelTest
    public void testCreateEmptyDirVolumeWithNullSizeLimit() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", null, null);
        assertThat(volume.getEmptyDir().getSizeLimit(), is(nullValue()));
    }

    @ParallelTest
    public void testCreateEmptyDirVolumeWithEmptySizeLimit() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", "", null);
        assertThat(volume.getEmptyDir().getSizeLimit(), is(nullValue()));
    }

    @ParallelTest
    public void testValidVolumeNames() {
        assertThat(VolumeUtils.getValidVolumeName("my-user"), is("my-user"));
        assertThat(VolumeUtils.getValidVolumeName("my-0123456789012345678901234567890123456789012345678901234-user"),
                is("my-0123456789012345678901234567890123456789012345678901234-user"));
    }

    @ParallelTest
    public void testInvalidVolumeNames() {
        assertThat(VolumeUtils.getValidVolumeName("my.user"), is("my-user-4dbf077e"));
        assertThat(VolumeUtils.getValidVolumeName("my_user"), is("my-user-e10dbc61"));
        assertThat(VolumeUtils.getValidVolumeName("my-very-long-012345678901234567890123456789012345678901234567890123456789-user"),
                is("my-very-long-01234567890123456789012345678901234567890-12a964dc"));
        assertThat(VolumeUtils.getValidVolumeName("my-very-long-0123456789012345678901234567890123456789-1234567890123456789-user"),
                is("my-very-long-0123456789012345678901234567890123456789-348f6fec"));
        assertThat(VolumeUtils.getValidVolumeName("my.bridge.012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"),
                is("my-bridge-01234567890123456789012345678901234567890123-30a4fce1"));
        assertThat(VolumeUtils.getValidVolumeName("a.a"), is("a-a-5bd24583"));
        assertThat(VolumeUtils.getValidVolumeName("a"), is("a-86f7e437"));
    }

    @ParallelTest
    public void testCreateSecretVolumeWithValidName() {
        Volume volume = VolumeUtils.createSecretVolume("oauth-my-secret", "my-secret", true);
        assertThat(volume.getName(), is("oauth-my-secret"));
    }

    @ParallelTest
    public void testCreateSecretVolumeWithInvalidName() {
        Volume volume = VolumeUtils.createSecretVolume("oauth-my.secret", "my.secret", true);
        assertThat(volume.getName(), is("oauth-my-secret-b744ae5a"));
    }

    @ParallelTest
    public void testCreateConfigMapVolumeWithValidName() {
        Volume volume = VolumeUtils.createConfigMapVolume("oauth-my-cm", "my-cm");
        assertThat(volume.getName(), is("oauth-my-cm"));
    }

    @ParallelTest
    public void testCreateConfigMapVolumeWithInvalidName() {
        Volume volume = VolumeUtils.createConfigMapVolume("oauth-my.cm", "my.cm");
        assertThat(volume.getName(), is("oauth-my-cm-62fdd747"));
    }

    @ParallelTest
    public void testCreateConfigMapVolumeWithItems() {
        Volume volume = VolumeUtils.createConfigMapVolume("my-cm-volume", "my-cm", Collections.singletonMap("fileName.txt", "/path/to/fileName.txt"));

        assertThat(volume.getName(), is("my-cm-volume"));
        assertThat(volume.getConfigMap().getName(), is("my-cm"));
        assertThat(volume.getConfigMap().getItems().size(), is(1));
        assertThat(volume.getConfigMap().getItems().get(0), is(new KeyToPathBuilder().withKey("fileName.txt").withPath("/path/to/fileName.txt").build()));
    }

    @ParallelTest
    public void testCreatePvcVolume()   {
        Volume volumeFromPvc = VolumeUtils.createPvcVolume("my-volume", "my-pvc");

        assertThat(volumeFromPvc.getName(), is("my-volume"));
        assertThat(volumeFromPvc.getPersistentVolumeClaim(), is(notNullValue()));
        assertThat(volumeFromPvc.getPersistentVolumeClaim().getClaimName(), is("my-pvc"));
    }

    ////////////////////
    // PodSet volume tests
    ////////////////////

    @ParallelTest
    public void testPodSetVolumesWithJbod()    {
        List<Volume> volumes = VolumeUtils.createPodSetVolumes("my-pod", JBOD_STORAGE, false);

        assertThat(volumes.size(), is(2));
        assertThat(volumes.get(0).getName(), is("data-0"));
        assertThat(volumes.get(0).getPersistentVolumeClaim().getClaimName(), is("data-0-my-pod"));
        assertThat(volumes.get(1).getName(), is("data-1"));
        assertThat(volumes.get(1).getPersistentVolumeClaim().getClaimName(), is("data-1-my-pod"));
    }

    @ParallelTest
    public void testPodSetVolumesWithJbodWithEphemeral()    {
        List<Volume> volumes = VolumeUtils.createPodSetVolumes("my-pod", JBOD_STORAGE_WITH_EPHEMERAL, false);

        assertThat(volumes.size(), is(2));
        assertThat(volumes.get(0).getName(), is("data-0"));
        assertThat(volumes.get(0).getPersistentVolumeClaim().getClaimName(), is("data-0-my-pod"));
        assertThat(volumes.get(1).getName(), is("data-1"));
        assertThat(volumes.get(1).getEmptyDir(), is(notNullValue()));
    }

    @ParallelTest
    public void testPodSetVolumesWithJbodWithoutId()    {
        InvalidResourceException ex = Assertions.assertThrows(
                InvalidResourceException.class,
                () -> VolumeUtils.createPodSetVolumes("my-pod", JBOD_STORAGE_WITHOUT_ID, false)
        );

        assertThat(ex.getMessage(), is("The 'id' property is required for volumes in JBOD storage."));
    }

    @ParallelTest
    public void testPodSetVolumesPersistentClaimOnly()    {
        List<Volume> volumes = VolumeUtils.createPodSetVolumes("my-pod", PERSISTENT_CLAIM_STORAGE, false);

        assertThat(volumes.size(), is(1));
        assertThat(volumes.get(0).getName(), is("data"));
        assertThat(volumes.get(0).getPersistentVolumeClaim().getClaimName(), is("data-my-pod"));
    }

    @ParallelTest
    public void testPodSetVolumesPersistentClaimWithId()    {
        List<Volume> volumes = VolumeUtils.createPodSetVolumes("my-pod", PERSISTENT_CLAIM_STORAGE_WITH_ID, false);

        assertThat(volumes.size(), is(1));
        assertThat(volumes.get(0).getName(), is("data"));
        assertThat(volumes.get(0).getPersistentVolumeClaim().getClaimName(), is("data-my-pod"));
    }

    @ParallelTest
    public void testPodSetVolumesEphemeralOnly()    {
        List<Volume> volumes = VolumeUtils.createPodSetVolumes("my-pod", EPHEMERAL_STORAGE, false);

        assertThat(volumes.size(), is(1));
        assertThat(volumes.get(0).getName(), is("data"));
        assertThat(volumes.get(0).getEmptyDir(), is(notNullValue()));
    }
}
