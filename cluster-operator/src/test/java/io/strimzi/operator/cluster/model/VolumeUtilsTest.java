/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import io.fabric8.kubernetes.api.model.Volume;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ParallelSuite
public class VolumeUtilsTest {
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
}
