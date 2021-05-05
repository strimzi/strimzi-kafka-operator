/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.generators.RndWithoutRepetition;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@ParallelSuite
public class CruiseControlBase {

    protected static final String HOST = "localhost";
    // static because we want's only one class of PORT_NUMBERS...
    protected static final RndWithoutRepetition PORT_NUMBERS = new RndWithoutRepetition(1080, 1100);
}
