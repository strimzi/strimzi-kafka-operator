/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.ReconciliationLogger;

class TopicOperatorEventHandler implements ResourceEventHandler<KafkaTopic> {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicOperatorEventHandler.class);

    private final BatchingLoop queue;
    private final boolean useFinalizer;

    public TopicOperatorEventHandler(BatchingLoop queue, boolean useFinalizer) {
        this.queue = queue;
        this.useFinalizer = useFinalizer;
    }

    @Override
    public void onAdd(KafkaTopic obj) {
        LOGGER.debugOp("Informed of add {}", obj);
        queue.offer(new TopicUpsert(System.nanoTime(), obj.getMetadata().getNamespace(),
                obj.getMetadata().getName(),
                obj.getMetadata().getResourceVersion()));
    }

    @Override
    public void onUpdate(KafkaTopic oldObj, KafkaTopic newObj) {
        String trigger = oldObj.equals(newObj) ? "resync" : "update";
        LOGGER.debugOp("Informed of {} {}", trigger, newObj);
        queue.offer(new TopicUpsert(System.nanoTime(), newObj.getMetadata().getNamespace(),
                newObj.getMetadata().getName(),
                newObj.getMetadata().getResourceVersion()));

    }

    @Override
    public void onDelete(KafkaTopic obj, boolean deletedFinalStateUnknown) {
        if (useFinalizer) {
            LOGGER.debugOp("Ignoring of delete {} (using finalizers)", obj);
        } else {
            LOGGER.debugOp("Informed of delete {}", obj);
            queue.offer(new TopicDelete(System.nanoTime(), obj));
        }
    }
}
