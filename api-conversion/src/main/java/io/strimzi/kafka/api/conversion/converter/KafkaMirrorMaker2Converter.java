/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Spec;

public class KafkaMirrorMaker2Converter extends AbstractSpecableConverter<KafkaMirrorMaker2> {
    public KafkaMirrorMaker2Converter() {
        super(KafkaMirrorMaker2Spec.class, "mirror-maker-2");
    }

    @Override
    public Class<KafkaMirrorMaker2> crClass() {
        return KafkaMirrorMaker2.class;
    }
}

