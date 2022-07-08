/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import org.junit.jupiter.api.Test;

import static io.strimzi.operator.cluster.model.RestartReason.CA_CERT_HAS_OLD_GENERATION;
import static io.strimzi.operator.cluster.model.RestartReason.FILE_SYSTEM_RESIZE_NEEDED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class RestartReasonTest {

    @Test
    void testPascalCasedReason() {
        assertThat(CA_CERT_HAS_OLD_GENERATION.pascalCased(), is("CaCertHasOldGeneration"));
        assertThat(FILE_SYSTEM_RESIZE_NEEDED.pascalCased(), is("FileSystemResizeNeeded"));
    }
}