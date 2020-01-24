/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Volume;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class VolumeUtilsTest {

    @Test
    public void testCreateEmptyDirVolumeWithSizeLimit() {
        Volume volume = VolumeUtils.createEmptyDirVolume("bar", "1Gi");
        assertThat(volume.getEmptyDir().getSizeLimit().getAmount(), is("1Gi"));
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
}
