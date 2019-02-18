/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.hamcrest.BaseMatcher;

import java.io.IOException;

/**
 * <p>A CmMatcher is custom matcher to check values for parameters
 * in the ConfigMap of tested cluster.</p>
 */
public class CmMatcher extends BaseMatcher<String> {

    private YAMLMapper mapper = new YAMLMapper();
    private String key;
    private String value;

    public CmMatcher(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean matches(Object actualValue) {
        try {
            JsonNode node = mapper.readTree(String.valueOf(actualValue));
            return node.get("data").get(key).asText().equals(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void describeTo(org.hamcrest.Description description) {
        description.appendText("key: " + key + " with value :" + value);
    }
}
