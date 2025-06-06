/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

public class ConfigMapOperatorServerSideApplyTest extends ConfigMapOperatorTest {
    @Override
    protected boolean useServerSideApply() {
        return true;
    }
}
