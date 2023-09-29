/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common.model;

/**
 * Exception indicating that some configuration parameters are invalid
 */
public class InvalidConfigParameterException extends InvalidResourceException {
    /**
     * Key with the invalid configuration
     */
    private final String key;

    /**
     * Constructs the exception
     *
     * @param key       Key of the invalid config parameter
     * @param message   Message explaining what is invalid
     */
    public InvalidConfigParameterException(String key, String message) {
        super(key + ": " + message);
        this.key = key;
    }

    /**
     * @return  Return the key which was invalid
     */
    public String getKey() {
        return key;
    }
}
