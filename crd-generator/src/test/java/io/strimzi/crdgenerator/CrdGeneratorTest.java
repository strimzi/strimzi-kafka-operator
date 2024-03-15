/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.KubeVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrdGeneratorTest {

    private final CrdGenerator.Reporter crdGeneratorReporter = new CrdGenerator.Reporter() {
        @Override
        public void warn(String s) {
        }

        @Override
        public void err(String err) {
            if (err.contains("@JsonInclude") || err.contains("@JsonPropertyOrder")) {
                // Currently we're only interested in testing @JsonInclude and @JsonPropertyOrder errors
                // As we would otherwise need to add dependencies to test HashCode, Equals, ToString, Builder etc.
                errors.add(err);
            }
        }
    };
    private final Set<String> errors = new HashSet<>();

    @BeforeEach
    public void beforeEachTest() {
        errors.clear();
    }

    @Test
    void simpleTest() throws IOException {
        StringWriter w = new StringWriter();
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_16_PLUS, ApiVersion.V1, CrdGenerator.YAML_MAPPER,
                emptyMap(), crdGeneratorReporter, emptyList(), null, null,
                new CrdGenerator.NoneConversionStrategy(), null);
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();

        assertTrue(errors.isEmpty(), "CrdGenerator should not report any errors: " + errors);
        assertEquals(CrdTestUtils.readResource("simpleTest.yaml"), s);
    }

    @Test
    void simpleTestWithoutDescriptions() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_16_PLUS, ApiVersion.V1, CrdGenerator.YAML_MAPPER,
                emptyMap(), crdGeneratorReporter, emptyList(), null, null,
                new CrdGenerator.NoneConversionStrategy(), ApiVersion.parseRange("v1+"));
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();

        assertTrue(errors.isEmpty(), "CrdGenerator should not report any errors: " + errors);
        assertEquals(CrdTestUtils.readResource("simpleTestWithoutDescriptions.yaml"), s);
    }

    @Test
    void simpleTestWithSubResources() throws IOException {
        StringWriter w = new StringWriter();
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_16_PLUS, ApiVersion.V1, CrdGenerator.YAML_MAPPER,
                emptyMap(), crdGeneratorReporter, emptyList(), null, null,
                new CrdGenerator.NoneConversionStrategy(), null);
        crdGenerator.generate(ExampleWithSubresourcesCrd.class, w);
        String s = w.toString();

        assertTrue(errors.isEmpty(), "CrdGenerator should not report any errors: " + errors);
        assertEquals(CrdTestUtils.readResource("simpleTestWithSubresources.yaml"), s);
    }

    @Test
    void generateHelmMetadataLabels() throws IOException {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("app", "{{ template \"strimzi.name\" . }}");
        labels.put("chart", "{{ template \"strimzi.chart\" . }}");
        labels.put("component", "%plural%.%group%-crd");
        labels.put("release", "{{ .Release.Name }}");
        labels.put("heritage", "{{ .Release.Service }}");
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_16_PLUS, ApiVersion.V1, CrdGenerator.YAML_MAPPER,
                labels, crdGeneratorReporter, emptyList(), null, null,
                new CrdGenerator.NoneConversionStrategy(), null);

        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();

        assertTrue(errors.isEmpty(), "CrdGenerator should not report any errors: " + errors);
        assertEquals(CrdTestUtils.readResource("simpleTestHelmMetadata.yaml"), s);
    }

    @Test
    void versionedTest() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_16_PLUS, ApiVersion.V1, CrdGenerator.YAML_MAPPER,
                emptyMap(), crdGeneratorReporter, emptyList(), null, null,
                new CrdGenerator.NoneConversionStrategy(), null);

        StringWriter w = new StringWriter();
        crdGenerator.generate(VersionedExampleCrd.class, w);
        String s = w.toString();

        assertTrue(errors.isEmpty(), "CrdGenerator should not report any errors: " + errors);
        assertEquals(CrdTestUtils.readResource("versionedTest.yaml"), s);
    }

    @Test
    void simpleTestWithErrors() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_16_PLUS, ApiVersion.V1, CrdGenerator.YAML_MAPPER,
                emptyMap(), crdGeneratorReporter, emptyList(), null, null,
                new CrdGenerator.NoneConversionStrategy(), null);
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrdWithErrors.class, w);

        assertTrue(errors.contains("class io.strimzi.crdgenerator.ExampleCrdWithErrors is missing @JsonInclude"), errors.toString());
        assertFalse(errors.contains("class io.strimzi.crdgenerator.ExampleCrdWithErrors$ObjectProperty is missing @JsonInclude"), errors.toString());
        assertTrue(errors.contains("class io.strimzi.crdgenerator.ExampleCrdWithErrors is missing @JsonPropertyOrder"), errors.toString());
        assertFalse(errors.contains("class io.strimzi.crdgenerator.ExampleCrdWithErrors$ObjectProperty is missing @JsonPropertyOrder"), errors.toString());
        assertTrue(errors.contains("class io.strimzi.crdgenerator.ExampleCrdWithErrors$ObjectProperty has a property bar which is not in the @JsonPropertyOrder"), errors.toString());
    }
}
