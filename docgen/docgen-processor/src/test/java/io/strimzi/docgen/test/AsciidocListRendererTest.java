/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.test;

import io.strimzi.docgen.model.TableInstanceModel;
import io.strimzi.docgen.renderer.AsciidocListRenderer;
import io.strimzi.docgen.renderer.AsciidocTableRenderer;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AsciidocListRendererTest extends AbstractRendererTest {

    final AsciidocListRenderer renderer = new AsciidocListRenderer();

    @Test
    public void testFileExtension() {
        assertEquals("adoc", renderer.fileFxtension());
    }

    @Test
    public void testRender() throws IOException {
        TableInstanceModel instance = tableInstance("name", "value", "pronunciation");
        instance.addRow(asList("foo", "The 1st metasyntactic variable", "fu"));
        instance.addRow(asList("bar", "The 2nd metasyntactic variable", "baa"));
        StringWriter sw = new StringWriter();
        renderer.render(instance, sw);
        assertEquals(
                "foo::\n" +
                        "  The 1st metasyntactic variable\n" +
                        "\n" +
                        "  fu\n" +
                        "\n" +
                        "bar::\n" +
                        "  The 2nd metasyntactic variable\n" +
                        "\n" +
                        "  baa\n" +
                        "\n", sw.toString());
    }

}
