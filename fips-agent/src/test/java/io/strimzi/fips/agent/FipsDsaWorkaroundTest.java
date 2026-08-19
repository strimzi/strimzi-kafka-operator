/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.fips.agent;

import org.junit.jupiter.api.Test;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FipsDsaWorkaroundTest {

    @Test
    void applyIsIdempotentWhenDsaAvailable() {
        assertDoesNotThrow(() -> KeyFactory.getInstance("DSA"));
        FipsDsaWorkaround.apply();
        assertDoesNotThrow(() -> KeyFactory.getInstance("DSA"));
    }

    @Test
    void applyRegistersDsaWhenMissing() {
        Provider sunProvider = Security.getProvider("SUN");
        try {
            if (sunProvider != null) {
                Security.removeProvider("SUN");
            }

            boolean dsaMissing;
            try {
                KeyFactory.getInstance("DSA");
                dsaMissing = false;
            } catch (NoSuchAlgorithmException e) {
                dsaMissing = true;
            }

            if (dsaMissing) {
                FipsDsaWorkaround.apply();
                assertNotNull(Security.getProvider("StrimziFipsDsaWorkaround"));
                assertDoesNotThrow(() -> KeyFactory.getInstance("DSA"));
            }
        } finally {
            Security.removeProvider("StrimziFipsDsaWorkaround");
            if (sunProvider != null) {
                Security.addProvider(sunProvider);
            }
        }
    }

    @Test
    void applyMultipleTimesDoesNotDuplicate() {
        FipsDsaWorkaround.apply();
        FipsDsaWorkaround.apply();
        assertDoesNotThrow(() -> KeyFactory.getInstance("DSA"));
    }
}
