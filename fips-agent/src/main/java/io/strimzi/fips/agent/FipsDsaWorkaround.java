/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.fips.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyFactorySpi;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;

/**
 * Workaround for Kafka's DefaultSslEngineFactory$PemStore unconditionally requiring DSA KeyFactory
 * at class-load time. On RHEL 10 / UBI 10 FIPS-enabled environments, OpenJDK automatically enters
 * strict FIPS mode and DSA is not available as it is not a FIPS-approved algorithm. This registers
 * a no-op DSA KeyFactory provider so the pre-loading check succeeds. Since Strimzi only uses
 * RSA/EC certificates, the DSA factory is never exercised.
 *
 * Remove this workaround when Kafka fixes the PemStore static initializer to gracefully skip
 * unavailable algorithms.
 */
class FipsDsaWorkaround {

    private static final Logger LOGGER = LoggerFactory.getLogger(FipsDsaWorkaround.class);

    private FipsDsaWorkaround() { }

    /**
     * Checks if DSA KeyFactory is available. If not (FIPS mode), registers a no-op provider.
     * Safe to call multiple times — no-ops if DSA is already available.
     */
    static void apply() {
        try {
            KeyFactory.getInstance("DSA");
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("DSA KeyFactory not available (FIPS mode detected). Registering no-op DSA provider to work around Kafka PemStore initialization bug.");
            Security.addProvider(new NoOpDsaProvider());
        }
    }

    /**
     * Minimal security provider that only registers a DSA KeyFactory service.
     */
    static class NoOpDsaProvider extends Provider {
        NoOpDsaProvider() {
            super("StrimziFipsDsaWorkaround", "1.0", "No-op DSA KeyFactory for FIPS compatibility with Kafka PemStore");
            put("KeyFactory.DSA", NoOpDsaKeyFactorySpi.class.getName());
        }
    }

    /**
     * KeyFactorySpi that satisfies KeyFactory.getInstance("DSA") but throws on actual use.
     */
    public static class NoOpDsaKeyFactorySpi extends KeyFactorySpi {
        /**
         * Constructor
         */
        public NoOpDsaKeyFactorySpi() { }

        @Override
        protected PublicKey engineGeneratePublic(KeySpec keySpec) throws InvalidKeySpecException {
            throw new InvalidKeySpecException("DSA is not supported in FIPS mode");
        }

        @Override
        protected PrivateKey engineGeneratePrivate(KeySpec keySpec) throws InvalidKeySpecException {
            throw new InvalidKeySpecException("DSA is not supported in FIPS mode");
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <T extends KeySpec> T engineGetKeySpec(Key key, Class<T> keySpec) throws InvalidKeySpecException {
            throw new InvalidKeySpecException("DSA is not supported in FIPS mode");
        }

        @Override
        protected Key engineTranslateKey(Key key) throws InvalidKeyException {
            throw new InvalidKeyException("DSA is not supported in FIPS mode");
        }
    }
}
