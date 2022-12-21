/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

/**
 * This class decouples the health check verticle from the startup sequence of the main Session verticle.
 * Previously the health check server was only started after everything else had successfully started,
 * but in low resource environments (especially Minikube), the Kafka Streams state store start up could exceed
 * the default liveness probe initial delay, and the topic operator would be unceremoniously killed.
 *
 * Now the health check verticle is started first, to allow the pod to start-up without being killed. However, the health
 * check's instantiation at the end of the future chains implicitly signalled success that everything had started up, as it would
 * never be started up otherwise.
 *
 * So this class captures the same implicit semantics of that sequencing, but explicitly so, to allow the pod to register
 * its liveness even if the state store is a bit slow starting up.
 *
 * alive defaults to true, but will be set to false when an error occurs during the startup sequence.
 * ready is false until the very end of the start-up sequence.
 */
public class TopicOperatorState {

    private volatile boolean alive = true;
    private volatile boolean ready = false;

    protected boolean isAlive() {
        return alive;
    }

    protected void setAlive(boolean alive) {
        this.alive = alive;
    }

    protected boolean isReady() {
        return ready;
    }

    protected void setReady(boolean ready) {
        this.ready = ready;
    }
}
