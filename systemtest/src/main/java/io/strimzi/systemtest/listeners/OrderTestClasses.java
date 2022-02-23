package io.strimzi.systemtest.listeners;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.StUtils;
import org.junit.jupiter.api.ClassDescriptor;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.ClassOrdererContext;

import java.util.Collections;
import java.util.Comparator;

/**
 * Provides an order of the test classes using {@code ClassDescriptorComparator}. The {@link io.strimzi.systemtest.annotations.ParallelSuite}
 * has highest priority. On the other hand {@link io.strimzi.systemtest.annotations.IsolatedSuite} has the lowest one.
 * The reason why ordering is beneficial for execution is that many threads, which are spawned by {@link java.util.concurrent.ForkJoinPool}
 * are blocked by {@link io.strimzi.systemtest.annotations.IsolatedSuite}. Completely re-ordering these tests, where first
 * we execute {@link io.strimzi.systemtest.annotations.ParallelSuite} would mean that we eliminate such problem and thus
 * utilize it.
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
public class OrderTestClasses implements ClassOrderer {

    private class ClassDescriptorComparator implements Comparator<ClassDescriptor> {

        @Override
        public int compare(ClassDescriptor classDescriptor, ClassDescriptor t1) {
            return compareTo(classDescriptor, t1);
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
