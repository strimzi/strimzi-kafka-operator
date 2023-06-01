/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import java.util.Map;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.StrimziFuture;

/**
 * Operations for {@code ConfigMap}s.
 */
public class ConfigMapOperator extends AbstractNamespacedResourceOperator<KubernetesClient, ConfigMap, ConfigMapList, Resource<ConfigMap>> {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ConfigMapOperator.class);

    /**
     * Constructor
     *
     * @param client The Kubernetes client
     */
    public ConfigMapOperator(KubernetesClient client) {
        super(client, "ConfigMap");
    }

    @Override
    protected MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>> operation() {
        return client.configMaps();
    }

    @Override
    protected StrimziFuture<ReconcileResult<ConfigMap>> internalUpdate(Reconciliation reconciliation, String namespace, String name, ConfigMap current, ConfigMap desired) {
        try {
            if (compareObjects(current.getData(), desired.getData())
                    && compareObjects(current.getMetadata().getName(), desired.getMetadata().getName())
                    && compareObjects(current.getMetadata().getNamespace(), desired.getMetadata().getNamespace())
                    && compareObjects(current.getMetadata().getAnnotations(), desired.getMetadata().getAnnotations())
                    && compareObjects(current.getMetadata().getLabels(), desired.getMetadata().getLabels())) {
                // Checking some metadata. We cannot check entire metadata object because it contains
                // timestamps which would cause restarting loop
                LOGGER.debugCr(reconciliation, "{} {} in namespace {} has not been patched because resources are equal", resourceKind, name, namespace);
                return StrimziFuture.completedFuture(ReconcileResult.noop(current));
            } else {
                return super.internalUpdate(reconciliation, namespace, name, current, desired);
            }
        } catch (Exception e) {
            LOGGER.errorCr(reconciliation, "Caught exception while patching {} {} in namespace {}", resourceKind, name, namespace, e);
            return StrimziFuture.failedFuture(e);
        }
    }

    private boolean compareObjects(Object a, Object b) {
        if (a == null && b instanceof Map<?, ?> bMap && bMap.size() == 0)
            return true;
        return !(a instanceof Map ^ b instanceof Map) && Objects.equals(a, b);
    }
}
