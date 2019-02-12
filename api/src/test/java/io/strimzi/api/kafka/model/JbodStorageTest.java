/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.Assert;
import org.junit.Test;

public class JbodStorageTest {

    private JbodStorage createJs(SingleVolumeStorage... storages) {
        JbodStorageBuilder builder = new JbodStorageBuilder();
        for (int i = 0; i < storages.length; ++i) {
            SingleVolumeStorage storage = storages[i];
            storage.setId(i);
            builder.addToVolumes(i, storage);
        }
        return builder.build();
    }

    private JbodStorage createValidJsOneOfEach() {
        return createJs(PersistentClaimStorageTest.createValidPcs(), EphemeralStorageTest.createValidEs());
    }

    private JbodStorage createValidJsTwoPersistent() {
        return createJs(PersistentClaimStorageTest.createValidPcs(), PersistentClaimStorageTest.createValidPcs());
    }

    private JbodStorage createValidJsJustEphemeral() {
        return createJs(EphemeralStorageTest.createValidEs());
    }

    @Test
    public void type() {
        Assert.assertEquals(Storage.TYPE_JBOD, createJs().getType());
    }

    @Test
    public void noInvalidityReason() {
        Assert.assertNull(createValidJsOneOfEach().invalidityReason());
    }

    @Test
    public void invalidPersistent() {
        Assert.assertEquals(PersistentClaimStorage.SIZE_REQUIRED_MESSAGE,
                createJs(PersistentClaimStorageTest.createNoSizePcs()).invalidityReason());
    }

    @Test
    public void invalidId() {
        JbodStorage missingId = new JbodStorageBuilder().addToVolumes(PersistentClaimStorageTest.createValidPcs()).build();
        Assert.assertEquals(JbodStorage.MISSING_ID, missingId.invalidityReason());
    }

    @Test
    public void containsPersistentStorage() {
        Assert.assertTrue(createValidJsOneOfEach().containsPersistentStorage());
        Assert.assertTrue(createValidJsTwoPersistent().containsPersistentStorage());
        Assert.assertFalse(createValidJsJustEphemeral().containsPersistentStorage());
    }


    @Test
    public void iterateEphemeralStorage() {
        new SuffixVisitor.Ephemeral().assertVisit(createValidJsOneOfEach(), 1);
        new SuffixVisitor.Ephemeral().assertVisit(createValidJsTwoPersistent(), 0);
        new SuffixVisitor.Ephemeral().assertVisit(createValidJsJustEphemeral(), 1);
    }

    @Test
    public void iteratePersistentClaimStorage() {
        new SuffixVisitor.PersistentClaim().assertVisit(createValidJsOneOfEach(), 1);
        new SuffixVisitor.PersistentClaim().assertVisit(createValidJsTwoPersistent(), 2);
        new SuffixVisitor.PersistentClaim().assertVisit(createValidJsJustEphemeral(), 0);
    }
}