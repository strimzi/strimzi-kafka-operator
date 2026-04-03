/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.api.annotations.ApiVersion;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DocGeneratorTest {
    @Test
    public void simpleTest() throws IOException {
        StringWriter w = new StringWriter();
        DocGenerator crdGenerator = new DocGenerator(ApiVersion.V1, 1, singletonList(ExampleCrd.class), w, new Linker("{KubeApiReferenceBase}"));
        crdGenerator.generate(ExampleCrd.class);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTest.adoc"), s);
    }
}
