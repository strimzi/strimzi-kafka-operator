/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PodSecurityProviderFactoryTest {
    @Test
    public void testExistingClass() {
        assertThat(PodSecurityProviderFactory.findProviderOrThrow("io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider"), is(instanceOf(RestrictedPodSecurityProvider.class)));
    }

    @Test
    public void testMissingClass() {
        Exception ex = assertThrows(InvalidConfigurationException.class, () -> PodSecurityProviderFactory.findProviderOrThrow("my.package.MyCustomPodSecurityProvider"));
        assertThat(ex.getMessage(), is("PodSecurityProvider my.package.MyCustomPodSecurityProvider was not found."));
    }
}
