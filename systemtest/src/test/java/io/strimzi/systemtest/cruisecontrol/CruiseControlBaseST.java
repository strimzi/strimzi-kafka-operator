/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.cruisecontrol;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;

public class CruiseControlBaseST extends AbstractST {

    protected static final String NAMESPACE = "cruise-control-base-namespace";

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        super.beforeAllMayOverride(extensionContext);

        installClusterWideClusterOperator(extensionContext, NAMESPACE, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }
}
