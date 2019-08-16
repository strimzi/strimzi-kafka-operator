/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import org.apache.kafka.clients.admin.AdminClient;

public interface AdminClientProvider {
    AdminClient createAdminClient(String hostname, Secret clusterCaCertSecret, Secret coKeySecret);
}
