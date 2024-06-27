/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.List;

import static java.lang.String.format;

/**
 * Cruise Control URL builder.
 */
class UrlBuilder {
    private String uri;
    private boolean firstParam;

    /**
     * Create a URL builder instance.
     *
     * @param hostname Server hostname.
     * @param port Server port.
     * @param endpoint Endpoint name.
     * @param ssl Whether SSL should be used.
     */
    public UrlBuilder(String hostname, int port, CruiseControlEndpoints endpoint, boolean ssl) {
        uri = format("%s://%s:%d%s?", ssl ? "https" : "http", hostname, port, endpoint);
        firstParam = true;
    }

    /**
     * Add query parameter with value to the path.
     *
     * @param param Cruise Control parameter.
     * @param value Parameter value.
     *
     * @return Instance of this builder.
     */
    public UrlBuilder withParameter(CruiseControlParameters param, String value) {
        if (!firstParam) {
            uri += "&";
        } else {
            firstParam = false;
        }
        try {
            uri += param.asPair(value);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
        return this;
    }

    /**
     * Add query parameter with multiple values to the path.
     *
     * @param param Cruise Control parameter.
     * @param values List of parameter values.
     *
     * @return  Instance of this builder.
     */
    public UrlBuilder withParameter(CruiseControlParameters param, List<String> values) {
        if (!firstParam) {
            uri += "&";
        } else {
            firstParam = false;
        }
        try {
            uri += param.asList(values);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
        return this;
    }

    /**
     * Build the URL.
     * 
     * @return The URL as URI instance.
     */
    public URI build() {
        return URI.create(uri);
    }
}
