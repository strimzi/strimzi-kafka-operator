/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

public class KafkaUserOperatorWithoutSSAMockTest extends KafkaUserOperatorMockTest {
    @Override
    protected boolean getSSA() {
        return false;
    }
}