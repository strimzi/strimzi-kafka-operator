/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.fabric8.kubernetes.api.model.HasMetadata;
import org.junit.jupiter.api.Test;

abstract class SpecableConverterTestBase<T extends AbstractSpecableConverter<U>, U extends HasMetadata> {

    abstract ExtConverters.ExtConverter<U> specableConverter();

    @Test
    public void testTolerationToV1Beta2() {
        convertTolerationsToV1beta2("kafka.strimzi.io/v1beta1");
        convertTolerationsToV1beta2("kafka.strimzi.io/v1alpha1");
    }

    protected abstract void convertTolerationsToV1beta2(String fromApiVersion);

    @Test
    public void testTolerationToV1Beta1() {
        convertTolerationsToV1beta1("kafka.strimzi.io/v1beta2");
    }

    protected abstract void convertTolerationsToV1beta1(String fromApiVersion);

    @Test
    public void testAffinityToV1Beta2() {
        convertAffinityToV1beta2("kafka.strimzi.io/v1beta1");
        convertAffinityToV1beta2("kafka.strimzi.io/v1alpha1");
    }

    protected abstract void convertAffinityToV1beta2(String fromApiVersion);

    @Test
    public void testAffinityToV1Beta1() {
        convertAffinityToV1beta1("kafka.strimzi.io/v1beta2");
    }

    protected abstract void convertAffinityToV1beta1(String fromApiVersion);
}