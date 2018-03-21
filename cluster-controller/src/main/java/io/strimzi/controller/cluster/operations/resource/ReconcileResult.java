/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

public class ReconcileResult {
    private static final ReconcileResult CREATED = new ReconcileResult() {
        public String toString() {
            return "CREATED";
        }
    };
    private static final ReconcileResult DELETED = new ReconcileResult() {
        public String toString() {
            return "DELETED";
        }
    };
    private static final ReconcileResult NOOP = new ReconcileResult() {
        public String toString() {
            return "NOOP";
        }
    };

    /** The resource was patched. */
    public static final ReconcileResult patched() {
        return new ReconcileResult() {
            public String toString() {
                return "PATCHED";
            }
        };
    }
    /** The resource was created. */
    public static final ReconcileResult created() {
        return CREATED;
    }
    /** The resource was deleted. */
    public static final ReconcileResult deleted() {
        return DELETED;
    }
    /** No action was performed. */
    public static final ReconcileResult noop() {
        return NOOP;
    }
    private ReconcileResult() {}
}
