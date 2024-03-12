/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import lombok.ToString;

/**
 * A {@code DefaultKubernetesResourceList<KafkaMirrorMaker>} required for using Fabric8 CRD support.
 */
@SuppressWarnings("deprecation") // Kafka Mirror Maker is deprecated
@ToString(callSuper = true)
public class KafkaMirrorMakerList extends DefaultKubernetesResourceList<KafkaMirrorMaker> {
    private static final long serialVersionUID = 1L;
}
