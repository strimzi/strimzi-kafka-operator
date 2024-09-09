/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Class with various utility methods for reading and writing files and objects
 */
public final class ReadWriteUtils {
    private static final Logger LOGGER = LogManager.getLogger(ReadWriteUtils.class);

    private ReadWriteUtils() {
        // All static methods
    }

    /**
     * Reads file frm the disk and returns it as a String
     *
     * @param file  The file that should be read
     *
     * @return  String with file content
     */
    public static String readFile(File file) {
        try {
            if (file == null) {
                return null;
            } else {
                return Files.readString(file.toPath());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads file frm the disk and returns it as a String
     *
     * @param filePath  Path to the file that should be read
     *
     * @return  String with file content
     */
    public static String readFile(String filePath) {
        return readFile(new File(filePath));
    }

    /**
     * Read the classpath resource with the given resourceName and return the content as a String
     *
     * @param cls           The class relative to which the resource will be loaded.
     * @param resourceName  The name of the file stored in resources
     *
     * @return  The resource content
     */
    public static String readFileFromResources(Class<?> cls, String resourceName) {
        try {
            URL url = cls.getResource(resourceName);
            if (url == null) {
                return null;
            } else {
                return Files.readString(Paths.get(
                        url.toURI()));
            }
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads an InputStream and returns its content as a String
     *
     * @param stream    InputStreams that should be read
     *
     * @return  String with the InputStream content
     */
    public static String readInputStream(InputStream stream) {
        StringBuilder textBuilder = new StringBuilder();
        try (Reader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            int character;

            while ((character = reader.read()) != -1) {
                textBuilder.append((char) character);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to read from InputStream", e);
        }

        return textBuilder.toString();
    }

    /**
     * Reads an object from a YAML file stored in resources
     *
     * @param resource  The name of the file in resources
     * @param c         The class from which resource path should be the file loaded
     *
     * @return  An object instance read from the file
     *
     * @param <T>   Type of the object
     */
    public static <T> T readObjectFromYamlFileInResources(String resource, Class<T> c) {
        return readObjectFromYamlFileInResources(resource, c, false);
    }

    /**
     * Reads an object from a YAML file stored in resources
     *
     * @param resource                  The name of the file in resources
     * @param c                         The class from which resource path should be the file loaded
     * @param ignoreUnknownProperties   Defines whether unknown properties should be ignored or if this method should
     *                                  fail with an exception
     *
     * @return  An object instance from the file
     *
     * @param <T>   Type of the resource
     */
    public static <T> T readObjectFromYamlFileInResources(String resource, Class<T> c, boolean ignoreUnknownProperties) {
        URL url = c.getResource(resource);
        if (url == null) {
            return null;
        }
        ObjectMapper mapper = new YAMLMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, !ignoreUnknownProperties);
        try {
            return mapper.readValue(url, c);
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads an object from YAML string
     *
     * @param yamlContent   String with the YAML of the object
     * @param c             The class representing the object
     *
     * @return  Returns the object instance based on the YAML
     *
     * @param <T>   Type of the object
     */
    public static <T> T readObjectFromYamlString(String yamlContent, Class<T> c) {
        try {
            ObjectMapper mapper = new YAMLMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
            return mapper.readValue(yamlContent, c);
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads an object from a YAML file
     *
     * @param yamlFile   File with the YAML
     * @param c          The class representing the object
     *
     * @return  Returns the object instance based on the YAML file
     *
     * @param <T>   Type of the object
     */
    public static <T> T readObjectFromYamlFilepath(File yamlFile, Class<T> c) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            return mapper.readValue(yamlFile, c);
        } catch (InvalidFormatException e) {
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads an object from a path to a YAML file
     *
     * @param yamlPath   Path to a YAML file
     * @param c          The class representing the object
     *
     * @return  Returns the object instance based on the YAML file path
     *
     * @param <T>   Type of the object
     */
    public static <T> T readObjectFromYamlFilepath(String yamlPath, Class<T> c) {
        return readObjectFromYamlFilepath(new File(yamlPath), c);
    }

    /**
     * Converts an object into YAML
     *
     * @param instance  The resource that should be converted to YAML
     *
     * @return  String with the YAML representation of the object
     *
     * @param <T>   Type of the object
     */
    public static <T> String writeObjectToYamlString(T instance) {
        try {
            ObjectMapper mapper = new YAMLMapper()
                    .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
                    .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            return mapper.writeValueAsString(instance);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts an object into YAML
     *
     * @param instance  The resource that should be converted to YAML
     *
     * @return  String with the YAML representation of the object
     *
     * @param <T>   Type of the object
     */
    public static <T> String writeObjectToJsonString(T instance) {
        try {
            ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
            return mapper.writeValueAsString(instance);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes text into a file
     *
     * @param filePath  Path of the file where the text will be written
     * @param text      The text that will be written into the file
     */
    public static void writeFile(Path filePath, String text) {
        try {
            Files.writeString(filePath, text, StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOGGER.warn("Exception during writing text in file", e);
        }
    }

    /**
     * Creates an empty file in the default temporary-file directory, using the given prefix and suffix.
     *
     * @param prefix    The prefix of the empty file (default: UUID).
     * @param suffix    The suffix of the empty file (default: .tmp).
     *
     * @return The empty file just created.
     */
    public static File tempFile(String prefix, String suffix) {
        File file;
        prefix = prefix == null ? UUID.randomUUID().toString() : prefix;
        suffix = suffix == null ? ".tmp" : suffix;
        try {
            file = Files.createTempFile(prefix, suffix).toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        file.deleteOnExit();
        return file;
    }

    /**
     * Get JSON content as string from resource file.
     *
     * TODO: Does the special handling here really matter? Can't we just use redFileFromResources?
     *
     * @param resourcePath Resource path.
     *
     * @return JSON content as string.
     */
    public static String readSingleLineJsonStringFromResourceFile(String resourcePath) {
        try {
            URI resourceURI = Objects.requireNonNull(TestUtils.class.getClassLoader().getResource(resourcePath)).toURI();
            try (Stream<String> lines = Files.lines(Paths.get(resourceURI), UTF_8)) {
                Optional<String> content = lines.reduce((x, y) -> x + y);

                if (content.isEmpty()) {
                    throw new IOException(format("File %s from resources was empty", resourcePath));
                }

                return content.get();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
