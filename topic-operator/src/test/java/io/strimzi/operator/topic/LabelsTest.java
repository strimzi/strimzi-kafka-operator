/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LabelsTest {

    @Test
    public void testOddNumberOfArgumentsThrows() {
        assertThrows(IllegalArgumentException.class, () -> new Labels("foo"));
        assertThrows(IllegalArgumentException.class, () -> new Labels("foo", "1", "bar"));
    }

    /**
     * See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
     * and https://github.com/kubernetes/community/blob/master/contributors/design-proposals/architecture/identifiers.md
     */
    @Test
    public void testParser() {
        assertParseThrows("foo");

        assertParseThrows("foo=bar,");

        // Disallow these for now
        assertParseThrows("foo==bar");
        assertParseThrows("foo!=bar");
        assertParseThrows("foo=bar,");
        assertParseThrows("foo=bar,,");
        assertParseThrows(",foo=bar");
        assertParseThrows(",,foo=bar");
        assertParseThrows("8/foo=bar");
        assertParseThrows("//foo=bar");
        assertParseThrows("a/b/foo=bar");
        assertParseThrows("foo=bar/");

        assertDoesNotThrow(() -> {
            // label values can be empty
            Labels.fromString("foo=");

            // single selector
            Labels.fromString("foo=bar");
            Labels.fromString("foo/bar=baz");
            Labels.fromString("foo.io/bar=baz");

            // multiple selectors
            Labels.fromString("foo=bar,name=gee");
            Labels.fromString("foo=bar,name=");
            Labels.fromString("foo=bar,lala/name=");
        });

    }

    private void assertParseThrows(String unparsable) {
        assertThrows(IllegalArgumentException.class, () -> Labels.fromString(unparsable));
    }

    @Test
    public void testLabels() {
        Labels p = new Labels("foo", "1", "bar", "2");
        Map<String, String> m = new HashMap<>(2);
        m.put("foo", "1");
        m.put("bar", "2");

        assertThat(p.labels(), is(m));
    }
}
