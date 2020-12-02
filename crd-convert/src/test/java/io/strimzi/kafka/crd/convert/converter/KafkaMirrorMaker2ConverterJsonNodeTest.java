/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.kafka.model.KafkaMirrorMaker2;

class KafkaMirrorMaker2ConverterJsonNodeTest extends KafkaMirrorMaker2ConverterTest {
    @Override
    ExtConverters.ExtConverter<KafkaMirrorMaker2> specableConverter() {
        return ExtConverters.nodeConverter(new KafkaMirrorMaker2Converter());
    }
}