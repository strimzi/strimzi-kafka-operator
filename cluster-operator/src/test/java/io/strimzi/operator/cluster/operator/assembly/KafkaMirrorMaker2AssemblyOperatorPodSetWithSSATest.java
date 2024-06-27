/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ManagedFieldsEntryBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
@ExtendWith(VertxExtension.class)
public class KafkaMirrorMaker2AssemblyOperatorPodSetWithSSATest extends KafkaMirrorMaker2AssemblyOperatorPodSetTest {
    @Override
    KafkaMirrorMaker2 initKafkaMirrorMaker() {
        return new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
                .withManagedFields(
                        new ManagedFieldsEntryBuilder()
                                .withManager("test")
                                .withOperation("Apply")
                                .withApiVersion("v1")
                                .withTime(OffsetDateTime.now(ZoneOffset.UTC).toString())
                                .withFieldsType("FieldsV1").withNewFieldsV1()
                                .addToAdditionalProperties(
                                        Map.of("f:metadata",
                                                Map.of("f:labels",
                                                        Map.of("f:test-label", Map.of())
                                                )
                                        )
                                )
                                .endFieldsV1()
                                .build()
                )
                .endMetadata()
                .withNewSpec()
                .withReplicas(3)
                .withClusters(List.of())
                .withMirrors(List.of())
                .endSpec()
                .build();
    }
}
