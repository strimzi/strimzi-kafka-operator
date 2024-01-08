/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.metrics.MetricsHolder;

import static io.strimzi.operator.common.Annotations.isReconciliationPausedWithAnnotation;

class TopicOperatorEventHandler implements ResourceEventHandler<KafkaTopic> {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicOperatorEventHandler.class);

    private final BatchingLoop queue;
    private final boolean useFinalizer;
    private final MetricsHolder metrics;
    private final String namespace;

    public TopicOperatorEventHandler(BatchingLoop queue, boolean useFinalizer, MetricsHolder metrics, String namespace) {
        this.queue = queue;
        this.useFinalizer = useFinalizer;
        this.metrics = metrics;
        this.namespace = namespace;
    }

    @Override
    public void onAdd(KafkaTopic obj) {
        LOGGER.debugOp("Informed of add {}", obj);
        metrics.resourceCounter(namespace).incrementAndGet();
        if (isReconciliationPausedWithAnnotation(obj)) {
            metrics.pausedResourceCounter(namespace).incrementAndGet();
        }
        queue.offer(new TopicUpsert(System.nanoTime(), obj.getMetadata().getNamespace(),
                obj.getMetadata().getName(),
                obj.getMetadata().getResourceVersion()));
    }

    @Override
    public void onUpdate(KafkaTopic oldObj, KafkaTopic newObj) {
        String trigger = oldObj.equals(newObj) ? "resync" : "update";
        LOGGER.debugOp("Informed of {} {}", trigger, newObj);
        if (isReconciliationPausedWithAnnotation(oldObj) && !isReconciliationPausedWithAnnotation(newObj)) {
            metrics.pausedResourceCounter(namespace).decrementAndGet();
        } else if (!isReconciliationPausedWithAnnotation(oldObj) && isReconciliationPausedWithAnnotation(newObj)) {
            metrics.pausedResourceCounter(namespace).incrementAndGet();
        }
        queue.offer(new TopicUpsert(System.nanoTime(), newObj.getMetadata().getNamespace(),
                newObj.getMetadata().getName(),
                newObj.getMetadata().getResourceVersion()));
    }

    @Override
    public void onDelete(KafkaTopic obj, boolean deletedFinalStateUnknown) {
        metrics.resourceCounter(namespace).decrementAndGet();
        if (isReconciliationPausedWithAnnotation(obj)) {
            metrics.pausedResourceCounter(namespace).decrementAndGet();
        }
        if (useFinalizer) {
            LOGGER.debugOp("Ignoring of delete {} (using finalizers)", obj);
        } else {
            LOGGER.debugOp("Informed of delete {}", obj);
            queue.offer(new TopicDelete(System.nanoTime(), obj));
        }
    }
}
