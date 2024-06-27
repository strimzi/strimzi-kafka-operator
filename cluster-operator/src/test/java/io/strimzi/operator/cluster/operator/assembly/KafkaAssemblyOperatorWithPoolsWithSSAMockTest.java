/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

public class KafkaAssemblyOperatorWithPoolsWithSSAMockTest extends KafkaAssemblyOperatorWithPoolsMockTest {
    @Override
    protected boolean getSSA() {
        return true;
    }
}
