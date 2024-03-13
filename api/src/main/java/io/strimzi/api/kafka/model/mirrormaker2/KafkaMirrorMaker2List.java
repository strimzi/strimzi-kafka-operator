/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker2;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;
import lombok.ToString;

/**
 * A {@code DefaultKubernetesResourceList<KafkaMirrorMaker2>} required for using Fabric8 CRD support.
 */
@ToString(callSuper = true)
public class KafkaMirrorMaker2List extends DefaultKubernetesResourceList<KafkaMirrorMaker2> {
    private static final long serialVersionUID = 1L;
}
