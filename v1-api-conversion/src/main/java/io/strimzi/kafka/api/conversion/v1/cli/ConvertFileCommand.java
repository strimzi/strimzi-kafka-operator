/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.MultipartConversions;
import io.strimzi.kafka.api.conversion.v1.converter.conversions.MultipartResource;
import io.strimzi.kafka.api.conversion.v1.utils.IoUtils;
import picocli.CommandLine;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * Command class for converting YAML files between the APIs
 */
@CommandLine.Command(name = "convert-file", aliases = {"cf"}, description = "Convert Custom Resources from YAML file")
public class ConvertFileCommand extends AbstractConversionCommand {
    private static final TypeReference<JsonNode> JSON_NODE_TYPE_REFERENCE = new TypeReference<>() { };

    @CommandLine.Option(names = {"-f", "--file"}, description = "Specifies the YAML file for the custom resource being converted", required = true)
    File inputFile;

    @CommandLine.ArgGroup
    Exclusive exclusive;

    static class Exclusive {
        @CommandLine.Option(names = {"-o", "--output"}, description = "Creates an output YAML file for the converted custom resource")
        File outputFile;

        @CommandLine.Option(names = {"--in-place"}, description = "Applies the changes directly to the input file specified by --file", defaultValue = "false")
        boolean inPlace;
    }

    /**
     * Converts the JsonNode corresponding to a single YAML document
     *
     * @param root  JsonNode with a single YAML resource
     *
     * @return      Converted JsonNode
     */
    protected JsonNode run(JsonNode root) {
        // We check that kind exists already when checking if it is a Strimzi resource. So this should not be null
        JsonNode kindNode = root.get("kind");
        getConverter(kindNode.asText()).convertTo(root, TO_API_VERSION);

        return root;
    }

    /**
     * Takes the byte array with the data from the input file, parses it into separate YAML documents, and if they are
     * Strimzi documents, it converts them. The conversion result is written into String and returned. When any multipart
     * conversions happen, the additional parts will be appended at the end.
     *
     * @param data  Byte array with the input YAML
     *
     * @return  String with the converted YAML
     *
     * @throws IOException  Throws IOException if conversion to YAML fails
     */
    protected String run(byte[] data) throws IOException {
        YAMLFactory yamlFactory = new YAMLFactory();
        YAMLMapper yamlMapper = new YAMLMapper(yamlFactory.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
        YAMLParser yamlParser = yamlFactory.createParser(data);
        List<JsonNode> docs = yamlMapper.readValues(yamlParser, JSON_NODE_TYPE_REFERENCE).readAll();

        Writer writer = new StringWriter();

        for (JsonNode doc : docs)   {
            JsonNode result;

            if (isStrimziResource(doc)) {
                result = run(doc);
            } else {
                result = doc;
            }

            JsonGenerator generator = yamlMapper.getFactory().createGenerator(writer);
            yamlMapper.writeTree(generator, result);
            writer.write(System.lineSeparator());
            handleMultipartResources(yamlMapper, writer);
        }

        return writer.toString();
    }

    /**
     * Checks if the YAML resource is a Strimzi resource by checking the API version and Kind
     *
     * @param document  The JsonNode that should be checked
     *
     * @return          True if the JsonNode is a Strimzi resource. False otherwise.
     */
    private boolean isStrimziResource(JsonNode document) {
        JsonNode apiVersion = document.get("apiVersion");
        JsonNode kind = document.get("kind");

        if (apiVersion == null || apiVersion.isNull()) {
            throw new IllegalArgumentException("Input YAML is missing 'apiVersion' node: " + document);
        }

        if (kind == null || kind.isNull()) {
            throw new IllegalArgumentException("Input YAML is missing 'kind' node: " + document);
        }

        return STRIMZI_KINDS.contains(kind.asText()) && apiVersion.asText().startsWith(STRIMZI_GROUPS.get(kind.asText()));
    }

    /**
     * When multipart conversions occurred (conversion which resulted in multiple YAMLs), we write it into the same
     * output file as separate YAML documents.
     *
     * @param yamlMapper    YAML Mapper instance
     * @param writer        Writer instance
     *
     * @throws IOException  Throws IOException if conversion to YAML fails
     */
    private void handleMultipartResources(YAMLMapper yamlMapper, Writer writer) throws IOException {
        try {
            List<MultipartResource> resources = MultipartConversions.get().getResources();

            for (MultipartResource resource : resources) {
                writer.write(yamlMapper.writeValueAsString(resource.resource()));
            }
        } finally {
            MultipartConversions.remove();
        }
    }


    /**
     * Reads the data from the input file into a byte array, converts them, and writes it into the output file
     */
    @Override
    public void run() {
        try {
            byte[] data;

            if (inputFile != null) {
                data = IoUtils.toBytes(new FileInputStream(inputFile));
            } else {
                throw new IllegalArgumentException("Missing input YAML file!");
            }

            // If an in-place update is enabled, an output file will be the input file
            if (exclusive != null && exclusive.inPlace) {
                exclusive.outputFile = inputFile;
            }

            if (debug) {
                log.info("Content of the input YAML file: {}", IoUtils.toString(data));
            }

            // Convert the YAML file
            String result = run(data);

            // Write converted YAML to file
            if (result != null) {
                if (debug) {
                    log.info("Result of the conversion: {}", result);
                }

                if (exclusive != null && exclusive.outputFile != null) {
                    Files.copy(
                            new ByteArrayInputStream(IoUtils.toBytes(result)),
                            exclusive.outputFile.toPath(),
                            StandardCopyOption.REPLACE_EXISTING
                    );
                } else {
                    println(result);
                }
            } else {
                throw new RuntimeException("Result is null - something went wrong!");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
