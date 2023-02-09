/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import java.util.HashMap;
import java.util.Map;

/**
 * User Operator Configuration Builder class
 */
public class UserOperatorConfigBuilder {

    private final Map<String, String> map;

    protected  UserOperatorConfigBuilder() {
        this.map = new HashMap<>();
    }

    protected  UserOperatorConfigBuilder(UserOperatorConfig config) {
        this.map = new HashMap<>();

        for (UserOperatorConfig.ConfigParameter<?> configParameter: UserOperatorConfig.values()) {
            if (configParameter.key().equals(UserOperatorConfig.STRIMZI_LABELS)) {
                map.put(configParameter.key(), config.get(UserOperatorConfig.LABELS).toSelectorString());
            } else {
                map.put(configParameter.key(), String.valueOf(config.get(configParameter)));
            }
        }
    }

    protected UserOperatorConfigBuilder with(String key, String value) {
        map.put(key, value);
        return this;
    }

    protected UserOperatorConfig build() {
        return new UserOperatorConfig(this.map);
    }
}
