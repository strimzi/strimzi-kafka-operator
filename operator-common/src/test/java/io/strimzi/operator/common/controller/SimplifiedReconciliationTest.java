/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.controller;

import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SimplifiedReconciliationTest {
    @Test
    public void testEquals() {
        SimplifiedReconciliation r1 = new SimplifiedReconciliation("kind", "my-namespace", "my-name", "watch");
        SimplifiedReconciliation r2 = new SimplifiedReconciliation("kind", "my-namespace", "my-name", "timer");
        SimplifiedReconciliation r3 = new SimplifiedReconciliation("kind", "my-namespace", "my-other-name", "watch");

        assertThat(r1.equals(r2), is(true));
        assertThat(r2.equals(r1), is(true));
        assertThat(r1.equals(r3), is(false));
    }

    @Test
    public void testToReconciliation() {
        SimplifiedReconciliation r1 = new SimplifiedReconciliation("kind", "my-namespace", "my-name", "watch");
        Reconciliation r = r1.toReconciliation();

        assertThat(r.kind(), is("kind"));
        assertThat(r.name(), is("my-name"));
        assertThat(r.namespace(), is("my-namespace"));
    }

    @Test
    public void testLockName() {
        SimplifiedReconciliation r1 = new SimplifiedReconciliation("kind", "my-namespace", "my-name", "watch");
        String lockName = r1.lockName();

        assertThat(lockName, is("kind::my-namespace::my-name"));
    }
}
