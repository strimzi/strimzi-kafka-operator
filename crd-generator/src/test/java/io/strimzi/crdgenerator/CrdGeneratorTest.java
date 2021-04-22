/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.KubeVersion;
import io.strimzi.api.annotations.VersionRange;
import org.junit.jupiter.api.Test;

import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CrdGeneratorTest {


    private final JsonPointer preV1PropertiesPath = JsonPointer.compile("/spec/validation/openAPIV3Schema/properties");
    private final JsonPointer v1VersionsPath = JsonPointer.compile("/spec/versions");
    private final JsonPointer v1SchemaPath = JsonPointer.compile("/schema/openAPIV3Schema/properties");

    @Test
    public void simpleTest() throws IOException {
        String s = generateCrdForVersions(ExampleCrd.class, KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1);
        assertEquals(CrdTestUtils.readResource("simpleTest.yaml"), s);
    }

    @Test
    public void simpleTestWithoutDescriptions() throws IOException {
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1, CrdGenerator.YAML_MAPPER, emptyMap(),
                new CrdGenerator.DefaultReporter(), emptyList(), null, null, new CrdGenerator.NoneConversionStrategy(), ApiVersion.parseRange("v1+"), null);
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTestWithoutDescriptions.yaml"), s);
    }

    @Test
    public void simpleTestWithSubresources() throws IOException {
        String s = generateCrdForVersions(ExampleWithSubresourcesCrd.class, KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1);
        assertEquals(CrdTestUtils.readResource("simpleTestWithSubresources.yaml"), s);
    }

    @Test
    public void generateHelmMetadataLabels() throws IOException {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("app", "{{ template \"strimzi.name\" . }}");
        labels.put("chart", "{{ template \"strimzi.chart\" . }}");
        labels.put("component", "%plural%.%group%-crd");
        labels.put("release", "{{ .Release.Name }}");
        labels.put("heritage", "{{ .Release.Service }}");
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1,
                CrdGenerator.YAML_MAPPER, labels,
                new CrdGenerator.DefaultReporter(), emptyList(), null, null, new CrdGenerator.NoneConversionStrategy(), null, null);
        StringWriter w = new StringWriter();
        crdGenerator.generate(ExampleCrd.class, w);
        String s = w.toString();
        assertEquals(CrdTestUtils.readResource("simpleTestHelmMetadata.yaml"), s);
    }

    @Test
    public void versionedTest() throws IOException {
        String s = generateCrdForVersions(VersionedExampleCrd.class, KubeVersion.parseRange("1.16+"), ApiVersion.V1BETA1);
        assertEquals(CrdTestUtils.readResource("versionedTest.yaml"), s);
    }

    @Test
    public void kubeV1_11ErrorWithMultiVersions() throws IOException {
        Set<String> errors = new HashSet<>();
        CrdGenerator crdGenerator = new CrdGenerator(KubeVersion.parseRange("1.11+"), ApiVersion.V1BETA1,
                CrdGenerator.YAML_MAPPER, emptyMap(), new CrdGenerator.Reporter() {
                    @Override
                    public void warn(String s) {
                    }

                    @Override
                    public void err(String s) {
                        errors.add(s);
                    }
                },
                emptyList(), null, null, new CrdGenerator.NoneConversionStrategy(), null, null);
        StringWriter w = new StringWriter();
        crdGenerator.generate(VersionedExampleCrd.class, w);
        assertTrue(errors.contains("Multiple scales specified but 1.11 doesn't support schema per version"), errors.toString());
        assertTrue(errors.contains("Target kubernetes versions 1.11+ don't support schema-per-version, but multiple versions present on io.strimzi.crdgenerator.VersionedExampleCrd.ignored"), errors.toString());
        assertTrue(errors.contains("Target kubernetes versions 1.11+ don't support schema-per-version, but multiple versions present on io.strimzi.crdgenerator.VersionedExampleCrd.someInt"), errors.toString());
        // TODO there's a bunch more checks we need here.
        // In particular one about the use of @Alternative
    }


    @Test
    public void testDefaultsOnlyGeneratedForCrdApiV1() throws IOException {
        String v1Beta1 = generateCrdForVersions(DefaultValuesCrd.class, KubeVersion.V1_16_PLUS, ApiVersion.V1BETA1);
        String v1Beta2 = generateCrdForVersions(DefaultValuesCrd.class, KubeVersion.V1_16_PLUS, ApiVersion.V1BETA2);
        String v1 = generateCrdForVersions(DefaultValuesCrd.class,  KubeVersion.V1_16_PLUS, ApiVersion.V1);

        assertTrue(propertiesSpec(v1Beta1, ApiVersion.V1BETA1).findValues("default").isEmpty(), "Found default fields in v1beta1 where none should exist");
        assertTrue(propertiesSpec(v1Beta2, ApiVersion.V1BETA2).findValues("default").isEmpty(), "Found default fields in v1beta2 where none should exist");

        assertFalse(propertiesSpec(v1, ApiVersion.V1).findValues("default").isEmpty(), "No default fields found in v1");
    }

    @Test
    public void testDefaultsGeneratedCorrectly() throws IOException {
        String v1 = generateCrdForVersions(DefaultValuesCrd.class,  KubeVersion.V1_16_PLUS, ApiVersion.V1);
        JsonNode propertiesSpec = propertiesSpec(v1, ApiVersion.V1);

        assertEquals(parseInt(DefaultValuesCrd.DEFAULT_PRIMITIVE_SHORT), propertiesSpec.at("/defaultPrimitiveShort/default").asInt());
        assertEquals(parseInt(DefaultValuesCrd.DEFAULT_BOXED_SHORT), propertiesSpec.at("/defaultBoxedShort/default").asInt());

        assertEquals(parseInt(DefaultValuesCrd.DEFAULT_PRIMITIVE_INT), propertiesSpec.at("/defaultPrimitiveInt/default").asInt());
        assertEquals(parseInt(DefaultValuesCrd.DEFAULT_BOXED_INT), propertiesSpec.at("/defaultBoxedInt/default").asInt());

        assertEquals(parseInt(DefaultValuesCrd.DEFAULT_PRIMITIVE_LONG), propertiesSpec.at("/defaultPrimitiveLong/default").asInt());
        assertEquals(parseInt(DefaultValuesCrd.DEFAULT_BOXED_LONG), propertiesSpec.at("/defaultBoxedLong/default").asInt());

        assertEquals(DefaultValuesCrd.DEFAULT_STRING, propertiesSpec.at("/defaultString/default").asText());
        assertEquals(DefaultValuesCrd.DEFAULT_NORMAL_ENUM, propertiesSpec.at("/defaultNormalEnum/default").asText());
        assertEquals(DefaultValuesCrd.DEFAULT_CUSTOM_ENUM, propertiesSpec.at("/defaultCustomisedEnum/default").asText());
    }

    private JsonNode propertiesSpec(String generatedCrd, ApiVersion version) throws JsonProcessingException {
        JsonNode root = CrdGenerator.YAML_MAPPER.readTree(generatedCrd);
        JsonNode propertiesNode = null;
        if (version.compareTo(ApiVersion.V1) < 0) {
            propertiesNode = root.at(preV1PropertiesPath);
        } else {
            ArrayNode versionsArray = (ArrayNode) root.at(v1VersionsPath);
            for (JsonNode elem : versionsArray) {
                ObjectNode cast = (ObjectNode) elem;
                String versionName = cast.get("name").asText();
                if (ApiVersion.parse(versionName).compareTo(version) == 0) {
                    propertiesNode  = elem.at(v1SchemaPath);
                }
            }
        }

        //Sanity check that the json pointer actually returns a valid object.
        assertNotNull(propertiesNode);
        assertFalse(propertiesNode.getClass().isInstance(MissingNode.class));
        return propertiesNode;
    }

    private <T extends CustomResource<?, ?>> String generateCrdForVersions(Class<T> crdClass, VersionRange<KubeVersion> targetKubeVersions, ApiVersion crdApiVersion) throws IOException {
        CrdGenerator gen = new CrdGenerator(targetKubeVersions, crdApiVersion);
        StringWriter w = new StringWriter();
        gen.generate(crdClass, w);
        return w.toString();
    }
}
