/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class CrdGeneratorTest {

    private CrdGenerator crdGenerator = new CrdGenerator(new YAMLMapper());

    @Test
    public void simpleTest() throws IOException, URISyntaxException {
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTest.yaml"), s);
    }
}
