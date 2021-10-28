/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common;

import io.strimzi.operator.cluster.model.InvalidResourceException;

public class InvalidConfigParameterException extends InvalidResourceException {

    private String key;
    public InvalidConfigParameterException(String key, String message) {
        super(key + ": " + message);
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
