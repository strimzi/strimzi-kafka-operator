/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.Assert;

import java.util.function.BiConsumer;

class SuffixVisitor<S extends SingleVolumeStorage> implements BiConsumer<S, String> {
    static final String SUFFIXED = "suffixed";

    int visited;

    @Override
    public void accept(S svs, String id) {
        visited++;
        Assert.assertEquals(SUFFIXED + '-' + svs.getId(), id);
    }


    static class Ephemeral extends SuffixVisitor<EphemeralStorage> {
        void assertVisit(Storage storage, int expected) {
            storage.iterateEphemeralStorage(this, SUFFIXED);
            Assert.assertEquals(expected, visited);
        }
    }

    static class PersistentClaim extends SuffixVisitor<PersistentClaimStorage> {
        void assertVisit(Storage storage, int expected) {
            storage.iteratePersistentClaimStorage(this, SUFFIXED);
            Assert.assertEquals(expected, visited);
        }
    }
}
