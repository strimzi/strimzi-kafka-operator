/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.Assert;
import org.junit.Test;

public class PersistentClaimStorageTest {

    private static final String NO_SUFFIX = "no_suffix";
    private static final String SIZE = "10Gi";
    private static final String CLASS = "ClaimClass";

    int counter;

    static PersistentClaimStorage createValidPcs() {
        return new PersistentClaimStorageBuilder().withSize(SIZE).withStorageClass(CLASS).build();
    }

    public static PersistentClaimStorage createNoSizePcs() {
        return new PersistentClaimStorageBuilder().withStorageClass(CLASS).build();
    }

    @Test
    public void type() {
        Assert.assertEquals(Storage.TYPE_PERSISTENT_CLAIM, createValidPcs().getType());
    }

    @Test
    public void noInvalidityReason() {
        Assert.assertNull(createValidPcs().invalidityReason());
    }

    @Test
    public void sizeInvalidityReason() {
        Assert.assertEquals(PersistentClaimStorage.SIZE_REQUIRED_MESSAGE, createNoSizePcs().invalidityReason());
    }

    @Test
    public void containsPersistentStorage() {
        Assert.assertTrue(createValidPcs().containsPersistentStorage());
    }

    @Test
    public void iterateEphemeralStorage() {
        new NoSuffixVisitor.Ephemeral().assertVisit(createValidPcs(), 0);
    }

    @Test
    public void iteratePersistentClaimStorage() {
        new NoSuffixVisitor.PersistentClaim().assertVisit(createValidPcs(), 1);
    }

}