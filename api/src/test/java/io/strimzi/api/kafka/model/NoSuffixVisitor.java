/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import org.junit.Assert;

import java.util.function.BiConsumer;

class NoSuffixVisitor<S extends SingleVolumeStorage> implements BiConsumer<S, String> {
    static final String NO_SUFFIX = "no_suffix";

    int visited;

    @Override
    public void accept(S svs, String volume) {
        visited++;
        Assert.assertEquals(NO_SUFFIX, volume);
    }

    static class Ephemeral extends NoSuffixVisitor<EphemeralStorage> {
        void assertVisit(Storage storage, int expected) {
            storage.iterateEphemeralStorage(this, NO_SUFFIX);
            Assert.assertEquals(expected, visited);
        }
    }

    static class PersistentClaim extends NoSuffixVisitor<PersistentClaimStorage> {
        void assertVisit(Storage storage, int expected) {
            storage.iteratePersistentClaimStorage(this, NO_SUFFIX);
            Assert.assertEquals(expected, visited);
        }
    }
}
