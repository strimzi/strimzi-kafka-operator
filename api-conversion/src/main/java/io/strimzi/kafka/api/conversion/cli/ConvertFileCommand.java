/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import io.strimzi.kafka.api.conversion.converter.MultipartConversions;
import io.strimzi.kafka.api.conversion.converter.MultipartResource;
import io.strimzi.kafka.api.conversion.utils.IoUtil;
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

@CommandLine.Command(name = "convert-file", aliases = {"cf"}, description = "Convert Custom Resources from YAML file")
public class ConvertFileCommand extends AbstractConversionCommand {
    private static final TypeReference<JsonNode> JSON_NODE_TYPE_REFERENCE = new TypeReference<JsonNode>() { };

    @CommandLine.Option(names = {"-f", "--file"}, description = "The YAML file with the Strimzi Custom Resource which should be converted", required = true)
    File inputFile;

    @CommandLine.Option(names = {"-o", "--output"}, description = "The YAML file with the converted Strimzi Custom Resource (if not specified, the input file will be overwritten)")
    File outputFile;

    /**
     * Converts the JsonNode corresponding to a single YAML document
     *
     * @param root  JsonNode with a single YAML resource
     *
     * @return      Converted JsonNode
     */
    protected JsonNode run(JsonNode root) {
        // We check that kind exists already when checking if it is Strimzi resource. So this should not be null
        JsonNode kindNode = root.get("kind");
        getConverter(kindNode.asText()).convertTo(root, TO_API_VERSION);

        return root;
    }

    /**
     * Takes the byte array with the data from the input file, parses it into separate YAML documents and if they are
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
        }

        handleMultipartResources(yamlMapper, writer);

        return writer.toString();
    }

    /**
     * Checks if the YAML resource is a Strimzi resource by checking the API version and Kind
     *
     * @param document  The JsonNode which should be checked
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

        if (apiVersion.asText().startsWith(STRIMZI_API) && STRIMZI_KINDS.contains(kind.asText()))   {
            return true;
        }

        return false;
    }

    /**
     * When multi-part conversions occurred (conversion which resulted in multiple YAMLs), we write it into the same
     * output file as a separate YAML documents.
     *
     * @param yamlMapper    YAML Mapper instance
     * @param writer        Writer instance
     *
     * @throws IOException  Throws IOException if conversion to YAML fails
     */
    private void handleMultipartResources(YAMLMapper yamlMapper, Writer writer) throws IOException {
        List<MultipartResource> resources = MultipartConversions.get().getResources();

        for (MultipartResource resource : resources) {
            writer.write(yamlMapper.writeValueAsString(resource.getResource()));
        }

        MultipartConversions.remove();
    }

    @Override
    /*
     * Reads the data from the input file into a byte array, converts them and writes it into the output file
     */
    public void run() {
        try {
            byte[] data;

            if (inputFile != null) {
                data = IoUtil.toBytes(new FileInputStream(inputFile));
            } else {
                throw new IllegalArgumentException("Missing input YAML file!");
            }

            if (data == null) {
                throw new RuntimeException("Failed to read input file - something went wrong!");
            }

            if (outputFile == null) {
                outputFile = inputFile;
            }

            if (debug) {
                log.info("Content of the input YAML file: " + IoUtil.toString(data));
            }

            // Convert the YAML file
            String result = run(data);

            // Write converted YAML to file
            if (result != null) {
                if (debug) {
                    log.info("Result of the conversion: " + result);
                }

                Files.copy(
                        new ByteArrayInputStream(IoUtil.toBytes(result)),
                        outputFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING
                );
            } else {
                throw new RuntimeException("Result is null - something went wrong!");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
