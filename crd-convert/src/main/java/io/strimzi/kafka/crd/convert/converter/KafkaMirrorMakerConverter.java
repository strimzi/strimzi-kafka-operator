/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaMirrorMakerSpec;

public class KafkaMirrorMakerConverter extends AbstractSpecableConverter<KafkaMirrorMaker> {
    public KafkaMirrorMakerConverter() {
        super(KafkaMirrorMakerSpec.class, "mirror-maker");
    }

    @Override
    public Class<KafkaMirrorMaker> crClass() {
        return KafkaMirrorMaker.class;
    }
}

