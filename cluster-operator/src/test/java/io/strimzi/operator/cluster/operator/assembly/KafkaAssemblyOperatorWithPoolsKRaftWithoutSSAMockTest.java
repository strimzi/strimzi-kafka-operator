/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@SuppressWarnings("checkstyle:ClassFanOutComplexity")
public class KafkaAssemblyOperatorWithPoolsKRaftWithoutSSAMockTest extends KafkaAssemblyOperatorWithPoolsKRaftMockTest {
    @Override
    protected boolean getSSA() {
        return false;
    }
}
