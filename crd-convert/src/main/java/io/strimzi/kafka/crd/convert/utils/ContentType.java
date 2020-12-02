/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public enum ContentType {
    JSON("application/json", new JsonMapper(), "json", "jsn"),
    YAML("application/x-yaml", new YAMLMapper(), "yaml", "yml");

    public static class Extensions implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            return Arrays.stream(values())
                .map(ct -> ct.extensions)
                .flatMap(Collection::stream)
                .iterator();
        }
    }

    String contentType;
    ObjectMapper mapper;
    Set<String> extensions;

    ContentType(String contentType, ObjectMapper mapper, String... extensions) {
        this.contentType = contentType;
        this.mapper = mapper;
        this.extensions = new LinkedHashSet<>(Arrays.asList(extensions));
    }

    public String getContentType() {
        return contentType;
    }

    public ObjectMapper getMapper() {
        return mapper;
    }

    public String getExtension() {
        return extensions.iterator().next();
    }

    /**
     * Find content type by matching content type string.
     *
     * @param contentType the content type to match
     * @return found content type or null if not found
     */
    public static ContentType findByContentType(String contentType) {
        for (ContentType ct : values()) {
            if (ct.contentType.equals(contentType)) {
                return ct;
            }
        }
        return null;
    }

    /**
     * Find content type by matching file extension.
     *
     * @param ext the extension to match
     * @return found content type
     * @throws IllegalArgumentException if no matching content type is found
     */
    public static ContentType findByExtension(String ext) {
        for (ContentType ct : values()) {
            if (ct.extensions.contains(ext)) {
                return ct;
            }
        }
        throw new IllegalArgumentException("No such supported extension: " + ext);
    }
}
