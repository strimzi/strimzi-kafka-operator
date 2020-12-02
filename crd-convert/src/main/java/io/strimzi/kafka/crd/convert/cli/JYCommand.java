/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.cli;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.kafka.crd.convert.utils.ContentType;
import picocli.CommandLine;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Json or Yaml content.
 */
public abstract class JYCommand extends ContentCommand {

    @CommandLine.Option(
        names = {"-it", "--input-type"},
        description = "Content input type, e.g. json or yaml"
    )
    ContentType inputType;

    @CommandLine.Option(
        names = {"-ot", "--output-type"},
        description = "Content output type, e.g. json or yaml"
    )
    ContentType outputType;

    protected abstract JsonNode run(JsonNode root) throws IOException;

    @Override
    protected Object run(byte[] data) throws IOException {
        ContentType inCT = inputType;
        if (inCT == null && inputExtension != null) {
            inCT = ContentType.findByExtension(inputExtension);
        }
        if (inCT == null) {
            log.info("Using default YAML type ...");
            inCT = ContentType.YAML;
        }
        ObjectMapper mapper = inCT.getMapper();

        JsonNode result = run(mapper.readTree(data));

        ContentType outCT = outputType;
        if (outCT == null && outputExtension != null) {
            outCT = ContentType.findByExtension(outputExtension);
        }
        if (outCT == null) {
            outCT = inCT;
        }
        mapper = outCT.getMapper();
        Writer writer = new StringWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(writer);
        mapper.writeTree(generator, result); // output same type; json vs yaml
        return writer.toString();
    }
}
