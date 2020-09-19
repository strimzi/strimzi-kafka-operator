/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Quantity;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Volume;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class VolumeUtilsTest {

    @Test
    public void testCreateEmptyDirVolumeWithSizeLimit() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", "1Gi");
        assertThat(volume.getEmptyDir().getSizeLimit(), is(new Quantity("1", "Gi")));
    }

    @Test
    public void testCreateEmptyDirVolumeWithNullSizeLimit() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", null);
        assertThat(volume.getEmptyDir().getSizeLimit(), is(nullValue()));
    }

    @Test
    public void testCreateEmptyDirVolumeWithEmptySizeLimit() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", "");
        assertThat(volume.getEmptyDir().getSizeLimit(), is(nullValue()));
    }

    @Test
    public void testValidVolumeNames() {
        assertThat(VolumeUtils.getValidVolumeName("my-user"), is("my-user"));
        assertThat(VolumeUtils.getValidVolumeName("my-0123456789012345678901234567890123456789012345678901234-user"),
                is("my-0123456789012345678901234567890123456789012345678901234-user"));
    }

    @Test
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

    @Test
    public void testCreateSecretVolumeWithValidName() {
        Volume volume = VolumeUtils.createSecretVolume("oauth-my-secret", "my-secret", true);
        assertThat(volume.getName(), is("oauth-my-secret"));
    }

    @Test
    public void testCreateSecretVolumeWithInvalidName() {
        Volume volume = VolumeUtils.createSecretVolume("oauth-my.secret", "my.secret", true);
        assertThat(volume.getName(), is("oauth-my-secret-b744ae5a"));
    }

    @Test
    public void testCreateConfigMapVolumeWithValidName() {
        Volume volume = VolumeUtils.createConfigMapVolume("oauth-my-cm", "my-cm");
        assertThat(volume.getName(), is("oauth-my-cm"));
    }

    @Test
    public void testCreateConfigMapVolumeWithInvalidName() {
        Volume volume = VolumeUtils.createConfigMapVolume("oauth-my.cm", "my.cm");
        assertThat(volume.getName(), is("oauth-my-cm-62fdd747"));
    }
}
