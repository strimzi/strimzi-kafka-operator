/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LabelsTest {

    @Test
    public void testCtorError() {
        try {
            new Labels("foo");
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new Labels("foo", "1", "bar");
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    /**
     * See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
     * and https://github.com/kubernetes/community/blob/master/contributors/design-proposals/architecture/identifiers.md
     */
    @Test
    public void testParser() {
        asserParseFails("foo");

        asserParseFails("foo=bar,");

        // Disallow these for now
        asserParseFails("foo==bar");
        asserParseFails("foo!=bar");
        asserParseFails("foo=bar,");
        asserParseFails("foo=bar,,");
        asserParseFails(",foo=bar");
        asserParseFails(",,foo=bar");
        asserParseFails("8/foo=bar");
        asserParseFails("//foo=bar");
        asserParseFails("a/b/foo=bar");
        asserParseFails("foo=bar/");

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
    }

    private void asserParseFails(String foo) {
        try {
            Labels.fromString(foo);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testLabels() {
        Labels p = new Labels("foo", "1", "bar", "2");
        Map<String, String> m = new HashMap<>(2);
        m.put("foo", "1");
        m.put("bar", "2");
        assertEquals(m, p.labels());
    }
}
