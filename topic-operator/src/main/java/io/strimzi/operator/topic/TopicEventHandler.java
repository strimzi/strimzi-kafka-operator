/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.metrics.MetricsHolder;
import io.strimzi.operator.topic.model.TopicEvent.TopicDelete;
import io.strimzi.operator.topic.model.TopicEvent.TopicUpsert;

import java.util.Objects;

/**
 * Handler for {@link KafkaTopic} events.
 */
class TopicEventHandler implements ResourceEventHandler<KafkaTopic> {
    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicEventHandler.class);

    private final TopicOperatorConfig config;
    private final BatchingLoop queue;
    private final MetricsHolder metrics;

    public TopicEventHandler(TopicOperatorConfig config, BatchingLoop queue, MetricsHolder metrics) {
        this.config = config;
        this.queue = queue;
        this.metrics = metrics;
    }

    @Override
    public void onAdd(KafkaTopic obj) {
        LOGGER.debugOp("Informed about add event for topic {}", TopicOperatorUtil.topicName(obj));
        metrics.resourceCounter(config.namespace()).incrementAndGet();
        if (Annotations.isReconciliationPausedWithAnnotation(obj)) {
            metrics.pausedResourceCounter(config.namespace()).incrementAndGet();
        }
        queue.offer(new TopicUpsert(System.nanoTime(), obj.getMetadata().getNamespace(),
                obj.getMetadata().getName(),
                obj.getMetadata().getResourceVersion()));
    }

    @Override
    public void onUpdate(KafkaTopic oldObj, KafkaTopic newObj) {
        String trigger = Objects.equals(oldObj, newObj) ? "resync" : "update";
        if (trigger.equals("resync")) {
            LOGGER.infoOp("Triggering periodic reconciliation of {} resources for namespace {}", KafkaTopic.RESOURCE_KIND, config.namespace());
        }
        if (trigger.equals("update")) {
            LOGGER.debugOp("Informed about update event for topic {}", TopicOperatorUtil.topicName(newObj));
        }
        if (Annotations.isReconciliationPausedWithAnnotation(oldObj) && !Annotations.isReconciliationPausedWithAnnotation(newObj)) {
            metrics.pausedResourceCounter(config.namespace()).decrementAndGet();
        } else if (!Annotations.isReconciliationPausedWithAnnotation(oldObj) && Annotations.isReconciliationPausedWithAnnotation(newObj)) {
            metrics.pausedResourceCounter(config.namespace()).incrementAndGet();
        }
        queue.offer(new TopicUpsert(System.nanoTime(), newObj.getMetadata().getNamespace(),
                newObj.getMetadata().getName(),
                newObj.getMetadata().getResourceVersion()));
    }

    @Override
    public void onDelete(KafkaTopic obj, boolean deletedFinalStateUnknown) {
        LOGGER.debugOp("Informed about delete event for topic {}", TopicOperatorUtil.topicName(obj));
        metrics.resourceCounter(config.namespace()).decrementAndGet();
        if (Annotations.isReconciliationPausedWithAnnotation(obj)) {
            metrics.pausedResourceCounter(config.namespace()).decrementAndGet();
        }
        if (config.useFinalizer()) {
            LOGGER.debugOp("Ignoring deletion of {} (using finalizers)", TopicOperatorUtil.topicName(obj));
        } else {
            queue.offer(new TopicDelete(System.nanoTime(), obj));
        }
    }
}
