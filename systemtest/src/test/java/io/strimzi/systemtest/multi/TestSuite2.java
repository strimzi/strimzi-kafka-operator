/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.multi;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.ParallelSuite;
import io.strimzi.systemtest.annotations.ParallelTest;

@ParallelSuite
public class TestSuite2 extends AbstractST {

    @ParallelTest
    void test1() throws InterruptedException {
        Thread.sleep(2000);

    }

    @ParallelTest
    void test2() throws InterruptedException {
        Thread.sleep(2000);

    }

    @ParallelTest
    void test3() {
    }
}
