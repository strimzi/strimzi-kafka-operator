/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.server;

import java.io.IOException;

import com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConversionReviewTest {

    @Test
    public void testDeserialization() throws IOException {
        ConversionReview conversionReview = new JsonMapper().readValue(getClass().getResourceAsStream("reviewRequest.json"), ConversionReview.class);
        Assertions.assertEquals("705ab4f5-6393-11e8-b7cc-42010a800002", conversionReview.getRequest().uid);
        Assertions.assertEquals(1, conversionReview.getRequest().objects.size());
    }
}