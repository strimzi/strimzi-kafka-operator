/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.test;

import io.strimzi.docgen.model.TableInstanceModel;
import io.strimzi.docgen.renderer.AsciidocTableRenderer;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AsciidocTableRendererTest extends AbstractRendererTest {

    final AsciidocTableRenderer renderer = new AsciidocTableRenderer();

    @Test
    public void testFileExtension() {
        assertEquals("adoc", renderer.fileFxtension());
    }

    @Test
    public void testRender() throws IOException {
        TableInstanceModel instance = tableInstance("foo", "bar");
        instance.addRow(asList("value for foo", "BAR"));
        StringWriter sw = new StringWriter();
        renderer.render(instance, sw);
        assertEquals(
                "[options=\"header\"]\n" +
                        "|==============================\n" +
                        "|Column for foo |Column for bar\n" +
                        "|value for foo  |BAR           \n" +
                        "|==============================\n", sw.toString());
    }

}
