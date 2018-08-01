/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster;

public class InvalidConfigParameterException extends RuntimeException {

    private String key;
    public InvalidConfigParameterException(String key, String message) {
        super(key + message);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
