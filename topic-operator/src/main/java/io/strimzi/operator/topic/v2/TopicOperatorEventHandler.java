/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.v2;

import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.controller.SimplifiedReconciliation;

class TopicOperatorEventHandler implements ResourceEventHandler<KafkaTopic> {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(TopicOperatorEventHandler.class);

    private final BatchingLoop queue;

    public TopicOperatorEventHandler(BatchingLoop queue) {
        this.queue = queue;
    }

    @Override
    public void onAdd(KafkaTopic obj) {
        SimplifiedReconciliation reconciliation = new SimplifiedReconciliation("KafkaTopic", obj.getMetadata().getNamespace(),
                obj.getMetadata().getName(), "add");
        LOGGER.debugOp("Informed of add {}", obj);
        queue.offer(new TopicUpsert(System.nanoTime(), obj.getMetadata().getNamespace(),
                obj.getMetadata().getName(),
                obj.getMetadata().getResourceVersion()));
    }

    @Override
    public void onUpdate(KafkaTopic oldObj, KafkaTopic newObj) {
        String trigger = oldObj.equals(newObj) ? "resync" : "update";
        SimplifiedReconciliation reconciliation = new SimplifiedReconciliation("KafkaTopic", newObj.getMetadata().getNamespace(),
                newObj.getMetadata().getName(), trigger);
        LOGGER.debugOp("Informed of {} {}", trigger, newObj);
        queue.offer(new TopicUpsert(System.nanoTime(), newObj.getMetadata().getNamespace(),
                newObj.getMetadata().getName(),
                newObj.getMetadata().getResourceVersion()));

    }

    @Override
    public void onDelete(KafkaTopic obj, boolean deletedFinalStateUnknown) {
        SimplifiedReconciliation reconciliation = new SimplifiedReconciliation("KafkaTopic", obj.getMetadata().getNamespace(),
                obj.getMetadata().getName(), "delete");
        LOGGER.debugOp("Informed of delete {}", obj);
        queue.offer(new TopicDelete(System.nanoTime(), obj));
    }
}
