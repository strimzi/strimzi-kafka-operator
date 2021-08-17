/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

interface Cancellable {
    boolean isCancelled();

    void cancel();

    static Cancellable atomic() {
        return new Cancellable() {

            private volatile boolean cancelled = false;

            @Override
            public boolean isCancelled() {
                return cancelled;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        };
    }

    static Cancellable future(java.util.concurrent.Future<?> future) {
        return new Cancellable() {
            @Override
            public boolean isCancelled() {
                return future.isCancelled();
            }

            @Override
            public void cancel() {
                future.cancel(true);
            }
        };
    }
}
