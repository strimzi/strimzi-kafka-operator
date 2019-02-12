/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.Assert;
import org.junit.Test;

public class EphemeralStorageTest {

    static EphemeralStorage createValidEs() {
        return new EphemeralStorageBuilder().build();
    }

    @Test
    public void type() {
        Assert.assertEquals(Storage.TYPE_EPHEMERAL, createValidEs().getType());
    }

    @Test
    public void noInvalidityReason() {
        Assert.assertNull(createValidEs().invalidityReason());
    }

    @Test
    public void containsPersistentStorage() {
        Assert.assertFalse(createValidEs().containsPersistentStorage());
    }

    @Test
    public void iterateEphemeralStorage() {
        new NoSuffixVisitor.Ephemeral().assertVisit(createValidEs(), 1);
    }

    @Test
    public void iteratePersistentClaimStorage() {
        new NoSuffixVisitor.PersistentClaim().assertVisit(createValidEs(), 0);
    }
}