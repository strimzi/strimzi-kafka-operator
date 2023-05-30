/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.EnvVar;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Interface to be implemented for returning an instance
 * of the environment variables shared by all containers.
 */
public interface SharedEnvironmentProvider {
    /**
     * Shared environment variables names.
     */
    enum EnvVarName {
        /** Proxy to use for HTTP requests */
        HTTP_PROXY,
        /** Proxy to use for HTTPS requests */
        HTTPS_PROXY,
        /** Comma separated list of DNS suffixes or IP addresses that can be accessed without passing through the proxy */
        NO_PROXY,
        /** Disable FIPS mode by setting disabled as value */
        FIPS_MODE;

        /**
         * Returns the list of env var names.
         * @return list of env var names
         */
        public static List<String> names() {
            return Arrays.stream(EnvVarName.values()).map(Enum::name).toList();
        }
    }

    /**
     * Returns a read-only collection of the shared env vars.
     *
     * @return Env vars collection
     */
    Collection<EnvVar> variables();

    /**
     * Returns the shared env var value.
     *
     * @param name Env var name
     * @return Env var value or null
     */
    String value(String name);
}
