/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.listeners;

import io.strimzi.systemtest.utils.StUtils;
import org.junit.jupiter.api.ClassDescriptor;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.ClassOrdererContext;

import java.util.Collections;
import java.util.Comparator;

/**
 * Provides an order of the test classes using {@code ClassDescriptorComparator}. Naturally, the {@link io.strimzi.systemtest.annotations.ParallelSuite}
 * has the highest priority, and on the other hand, {@link io.strimzi.systemtest.annotations.IsolatedSuite} has the lowest one.
 * That means that test classes that contain {@link io.strimzi.systemtest.annotations.IsolatedSuite} will be executed last.
 * The reason why ordering is beneficial for execution is that {@link io.strimzi.systemtest.annotations.IsolatedSuite} may
 * block many threads, which {@link java.util.concurrent.ForkJoinPool} spawns (using a work-stealing algorithm).
 * In the scenario where we have the following test plan:
 *
 *      1. @ParallelSuite
 *      2. @ParallelSuite
 *      3. @IsolatedSuite
 *      4. @IsolatedSuite
 *      5. @ParallelSuite
 *      6. @ParallelSuite
 *
 * We configure fixed class-wide parallelism using only three threads. Meaning that the first two threads will run in parallel,
 * but the third one must way until these two threads complete their execution. If the first {@link io.strimzi.systemtest.annotations.ParallelSuite}
 * completes its execution, this thread gets assigned the following test class, {@link io.strimzi.systemtest.annotations.IsolatedSuite}
 * (Thread will be waiting for the second thread to finish its execution). This is not an optimal execution path.
 * Instead, we re-order such test classes before TestEngine runs them and eliminate blocked threads.
 *
 * For instance here is the run completely re-order of such test run. {@link io.strimzi.systemtest.annotations.IsolatedSuite}
 * are the last one to execute.
 *
 *  Following testclasses are selected for run:
 *  -> io.strimzi.systemtest.cruisecontrol.CruiseControlST
 *  -> io.strimzi.systemtest.cruisecontrol.CruiseControlConfigurationST
 *  -> io.strimzi.systemtest.cruisecontrol.CruiseControlApiST
 *  -> io.strimzi.systemtest.kafka.listeners.ListenersST
 *  -> io.strimzi.systemtest.kafka.listeners.MultipleListenersST
 *  -> io.strimzi.systemtest.kafka.ConfigProviderST
 *  -> io.strimzi.systemtest.kafka.dynamicconfiguration.DynamicConfSharedST
 *  -> io.strimzi.systemtest.kafka.dynamicconfiguration.DynamicConfST
 *  -> io.strimzi.systemtest.kafka.KafkaST
 *  -> io.strimzi.systemtest.bridge.HttpBridgeTlsST
 *  -> io.strimzi.systemtest.bridge.HttpBridgeScramShaST
 *  -> io.strimzi.systemtest.mirrormaker.MirrorMakerIsolatedST
 *  -> io.strimzi.systemtest.mirrormaker.MirrorMaker2IsolatedST
 *  -> io.strimzi.systemtest.connect.ConnectIsolatedST
 *  -> io.strimzi.systemtest.connect.ConnectBuilderIsolatedST
 *  -> io.strimzi.systemtest.bridge.HttpBridgeIsolatedST
 *  -> io.strimzi.systemtest.metrics.MetricsIsolatedST
 *  -> io.strimzi.systemtest.metrics.JmxIsolatedST
 *
 */
public class OrderTestSuites implements ClassOrderer {

    private class ClassDescriptorComparator implements Comparator<ClassDescriptor> {

        @Override
        public int compare(ClassDescriptor classDescriptor, ClassDescriptor otherDescriptor) {
            return compareTo(classDescriptor, otherDescriptor);
        }

        /**
         * Helper method, for comparing two {@code ClassDescriptor} objects. This is achieved by {@link io.strimzi.systemtest.annotations.IsolatedSuite}
         * annotation where such test suite has low priority.
         *
         * @param classDescriptor the first {@link ClassDescriptor} to be compared
         * @param otherDescriptor the other {@link ClassDescriptor} to be compared
         * @return 0 if classDescriptorName == otherDescriptorName or both descriptor has {@link io.strimzi.systemtest.annotations.IsolatedSuite};
         * -1 if classDescriptor does not contain {@link io.strimzi.systemtest.annotations.IsolatedSuite} and thus is &lt; otherDescriptorName;
         * 1 if classDescriptor contains {@link io.strimzi.systemtest.annotations.IsolatedSuite} and thus is &gt; otherDescriptorName.
         */
        private int compareTo(ClassDescriptor classDescriptor, ClassDescriptor otherDescriptor) {
            final String classDescriptorName = classDescriptor.getTestClass().getName();
            final String otherDescriptorName = otherDescriptor.getTestClass().getName();

            if (StUtils.isIsolatedSuite(classDescriptor) && !StUtils.isIsolatedSuite(otherDescriptor)) {
                return 1;
            } else if (!StUtils.isIsolatedSuite(classDescriptor) && StUtils.isIsolatedSuite(otherDescriptor)) {
                return -1;
            } else {
                // Both has @ParallelSuites or @IsolatedSuite
                return 0;
            }
        }
    }

    @Override
    public void orderClasses(ClassOrdererContext context) {
        Collections.sort(context.getClassDescriptors(), new ClassDescriptorComparator());
    }
}
