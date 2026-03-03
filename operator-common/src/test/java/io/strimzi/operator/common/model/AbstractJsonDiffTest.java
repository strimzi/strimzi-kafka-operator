/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AbstractJsonDiffTest {
    @Test
    public void testLookupPath() {
        try {
            JsonNode source = new ObjectMapper().readTree("""
                    {
                        "field1": "value1",
                        "field2": {
                            "subfield1": "subvalue1",
                            "subfield2": ["subvalue2", "subvalue3", "subvalue4"]
                        }
                    }
                    """);

            assertThat(AbstractJsonDiff.lookupPath(source, ""), is(source));
            assertThat(AbstractJsonDiff.lookupPath(source, "/"), is(source));
            assertThat(AbstractJsonDiff.lookupPath(source, "/field1"), is(source.get("field1")));
            assertThat(AbstractJsonDiff.lookupPath(source, "/field2"), is(source.get("field2")));
            assertThat(AbstractJsonDiff.lookupPath(source, "/field2/subfield2"), is(source.get("field2").get("subfield2")));
            assertThat(AbstractJsonDiff.lookupPath(source, "/field2/subfield2/1"), is(source.get("field2").get("subfield2").get(1)));
            assertThat(AbstractJsonDiff.lookupPath(source, "/field2/subfield2/13"), is(MissingNode.getInstance()));
        } catch (Exception e) { // Catches Exception instead of JsonProcessingException to avoid dependency on jackson-core just for the exception
            throw new RuntimeException(e);
        }
    }

}
