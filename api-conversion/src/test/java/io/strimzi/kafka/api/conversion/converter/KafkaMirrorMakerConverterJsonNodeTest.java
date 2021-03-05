/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.strimzi.api.kafka.model.KafkaMirrorMaker;

class KafkaMirrorMakerConverterJsonNodeTest extends KafkaMirrorMakerConverterTest {
    @Override
    ExtConverters.ExtConverter<KafkaMirrorMaker> specableConverter() {
        return ExtConverters.nodeConverter(new KafkaMirrorMakerConverter());
    }
}