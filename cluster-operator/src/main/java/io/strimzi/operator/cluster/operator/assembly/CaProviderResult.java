/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.common.ca.Ca;

/**
 * Result of creating and reconciling CA
 *
 * @param ca            The created CA instance
 * @param certSecret    The reconciled CA certificate secret
 */
public record CaProviderResult(Ca ca, Secret certSecret) { }
