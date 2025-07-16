/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacity;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.BrokerCapacityBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpec;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlSpecBuilder;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.annotations.ParallelTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CapacityConfigurationTest {
    private static final Map<String, ResourceRequirements> RESOURCES = createResources("400m", "0.5");
    private static final Set<NodeRef> NODES = Set.of(
            new NodeRef("my-cluster-brokers-0", 0, "brokers", false, true),
            new NodeRef("my-cluster-brokers-1", 1, "brokers", false, true),
            new NodeRef("my-cluster-brokers-2", 2, "brokers", false, true)
    );

    @ParallelTest
    public void testBrokerCapacity() {
        CruiseControlSpec spec = new CruiseControlSpecBuilder()
            .withNewBrokerCapacity()
                .withCpu("2575m")
                .withInboundNetwork("50000KB/s")
                .withOutboundNetwork("50000KB/s")
            .endBrokerCapacity()
            .build();

        Map<String, Storage> storage = Map.of(
            "brokers",
            new JbodStorageBuilder()
                .withVolumes(
                    new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withSize("50Gi")
                        .build(),
                    new PersistentClaimStorageBuilder()
                        .withId(1)
                        .withSize("60Gi")
                        .build()
                )
                .build()
        );

        CapacityConfiguration config = new CapacityConfiguration(Reconciliation.DUMMY_RECONCILIATION, spec, NODES, storage, RESOURCES);
        String actualCapacityConfig = config.toJson();
        String expectedCapacityConfig = """
                {
                  "brokerCapacities" : [ {
                    "brokerId" : 0,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-0/kafka-log0" : "51200.0",
                        "/var/lib/kafka/data-1/kafka-log0" : "61440.0"
                      },
                      "CPU" : {
                        "num.cores" : "2.575"
                      },
                      "NW_IN" : "48828.125",
                      "NW_OUT" : "48828.125"
                    },
                    "doc" : "Capacity for Broker 0"
                  }, {
                    "brokerId" : 1,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-1/kafka-log1" : "61440.0",
                        "/var/lib/kafka/data-0/kafka-log1" : "51200.0"
                      },
                      "CPU" : {
                        "num.cores" : "2.575"
                      },
                      "NW_IN" : "48828.125",
                      "NW_OUT" : "48828.125"
                    },
                    "doc" : "Capacity for Broker 1"
                  }, {
                    "brokerId" : 2,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-0/kafka-log2" : "51200.0",
                        "/var/lib/kafka/data-1/kafka-log2" : "61440.0"
                      },
                      "CPU" : {
                        "num.cores" : "2.575"
                      },
                      "NW_IN" : "48828.125",
                      "NW_OUT" : "48828.125"
                    },
                    "doc" : "Capacity for Broker 2"
                  } ]
                }""";

        assertEquals(expectedCapacityConfig, actualCapacityConfig);
    }

    @ParallelTest
    public void testBrokerCapacityOverrides() {
        CruiseControlSpec spec = new CruiseControlSpecBuilder()
            .withNewBrokerCapacity()
                .withCpu("2575m")
                .withInboundNetwork("50000KB/s")
                .addNewOverride()
                    .withBrokers(List.of(0, 1, 2, 0))
                    .withCpu("1.222")
                    .withInboundNetwork("25000KB/s")
                .endOverride()
                .addNewOverride()
                    .withBrokers(List.of(1))
                    .withInboundNetwork("10000KiB/s")
                    .withOutboundNetwork("15000KB/s")
                .endOverride()
            .endBrokerCapacity()
            .build();

        Map<String, Storage> storage = Map.of(
            "brokers",
            new PersistentClaimStorageBuilder()
                .withId(0)
                .withSize("50Gi")
                .build()
        );

        CapacityConfiguration config = new CapacityConfiguration(Reconciliation.DUMMY_RECONCILIATION, spec, NODES, storage, RESOURCES);
        String actualCapacityConfig = config.toJson();
        String expectedCapacityConfig = """
                {
                  "brokerCapacities" : [ {
                    "brokerId" : 0,
                    "capacity" : {
                      "DISK" : "51200.0",
                      "CPU" : {
                        "num.cores" : "1.222"
                      },
                      "NW_IN" : "24414.0625",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 0"
                  }, {
                    "brokerId" : 1,
                    "capacity" : {
                      "DISK" : "51200.0",
                      "CPU" : {
                        "num.cores" : "1.222"
                      },
                      "NW_IN" : "24414.0625",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 1"
                  }, {
                    "brokerId" : 2,
                    "capacity" : {
                      "DISK" : "51200.0",
                      "CPU" : {
                        "num.cores" : "1.222"
                      },
                      "NW_IN" : "24414.0625",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 2"
                  } ]
                }""";

        assertEquals(expectedCapacityConfig, actualCapacityConfig);
    }

    @ParallelTest
    public void testBrokerCapacityGeneratedCpu() {
        CruiseControlSpec spec = new CruiseControlSpecBuilder()
            .withNewBrokerCapacity()
                .withCpu("2575m")
                .withInboundNetwork("50000KB/s")
                .addNewOverride()
                    .withBrokers(List.of(0, 1, 2, 0))
                    .withCpu("1.222")
                    .withInboundNetwork("25000KB/s")
                .endOverride()
                .addNewOverride()
                    .withBrokers(List.of(1))
                    .withInboundNetwork("10000KiB/s")
                    .withOutboundNetwork("15000KB/s")
                .endOverride()
            .endBrokerCapacity()
            .build();

        Map<String, Storage> storage = Map.of(
            "brokers",
            new JbodStorageBuilder()
                .withVolumes(
                    new PersistentClaimStorageBuilder().withId(0).withSize("50Gi").build(),
                    new PersistentClaimStorageBuilder().withId(1).withSize("60Gi").build()
                )
                .build()
        );

        CapacityConfiguration config = new CapacityConfiguration(Reconciliation.DUMMY_RECONCILIATION, spec, NODES, storage, RESOURCES);
        String actualCapacityConfig = config.toJson();
        String expectedCapacityConfig = """
                {
                  "brokerCapacities" : [ {
                    "brokerId" : 0,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-0/kafka-log0" : "51200.0",
                        "/var/lib/kafka/data-1/kafka-log0" : "61440.0"
                      },
                      "CPU" : {
                        "num.cores" : "1.222"
                      },
                      "NW_IN" : "24414.0625",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 0"
                  }, {
                    "brokerId" : 1,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-1/kafka-log1" : "61440.0",
                        "/var/lib/kafka/data-0/kafka-log1" : "51200.0"
                      },
                      "CPU" : {
                        "num.cores" : "1.222"
                      },
                      "NW_IN" : "24414.0625",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 1"
                  }, {
                    "brokerId" : 2,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-0/kafka-log2" : "51200.0",
                        "/var/lib/kafka/data-1/kafka-log2" : "61440.0"
                      },
                      "CPU" : {
                        "num.cores" : "1.222"
                      },
                      "NW_IN" : "24414.0625",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 2"
                  } ]
                }""";

        assertEquals(expectedCapacityConfig, actualCapacityConfig);
    }

    @ParallelTest
    public void testBrokerCapacitiesWithPools() {
        Set<NodeRef> nodes = Set.of(
                new NodeRef("my-cluster-pool1-0", 0, "pool1", false, true),
                new NodeRef("my-cluster-pool1-1", 1, "pool1", false, true),
                new NodeRef("my-cluster-pool1-2", 2, "pool1", false, true),
                new NodeRef("my-cluster-pool2-10", 10, "pool2", false, true),
                new NodeRef("my-cluster-pool2-11", 11, "pool2", false, true),
                new NodeRef("my-cluster-pool2-12", 12, "pool2", false, true)
        );

        Map<String, Storage> storage = new HashMap<>();
        storage.put("pool1", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build()).build());
        storage.put("pool2", new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withId(1).withSize("1Ti").build()).build());

        Map<String, ResourceRequirements> resources = new HashMap<>();
        resources.put("pool1", new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("4"), "memory", new Quantity("16Gi"))).build());
        resources.put("pool2", new ResourceRequirementsBuilder().withLimits(Map.of("cpu", new Quantity("5"), "memory", new Quantity("20Gi"))).build());

        CapacityConfiguration config = new CapacityConfiguration(Reconciliation.DUMMY_RECONCILIATION, new CruiseControlSpec(), nodes, storage, resources);
        String actualCapacityConfig = config.toJson();
        String expectedCapacityConfig = """
                {
                  "brokerCapacities" : [ {
                    "brokerId" : 0,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-0/kafka-log0" : "102400.0"
                      },
                      "CPU" : {
                        "num.cores" : "4.0"
                      },
                      "NW_IN" : "10000.0",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 0"
                  }, {
                    "brokerId" : 1,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-0/kafka-log1" : "102400.0"
                      },
                      "CPU" : {
                        "num.cores" : "4.0"
                      },
                      "NW_IN" : "10000.0",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 1"
                  }, {
                    "brokerId" : 2,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-0/kafka-log2" : "102400.0"
                      },
                      "CPU" : {
                        "num.cores" : "4.0"
                      },
                      "NW_IN" : "10000.0",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 2"
                  }, {
                    "brokerId" : 10,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-1/kafka-log10" : "1048576.0"
                      },
                      "CPU" : {
                        "num.cores" : "5.0"
                      },
                      "NW_IN" : "10000.0",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 10"
                  }, {
                    "brokerId" : 11,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-1/kafka-log11" : "1048576.0"
                      },
                      "CPU" : {
                        "num.cores" : "5.0"
                      },
                      "NW_IN" : "10000.0",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 11"
                  }, {
                    "brokerId" : 12,
                    "capacity" : {
                      "DISK" : {
                        "/var/lib/kafka/data-1/kafka-log12" : "1048576.0"
                      },
                      "CPU" : {
                        "num.cores" : "5.0"
                      },
                      "NW_IN" : "10000.0",
                      "NW_OUT" : "10000.0"
                    },
                    "doc" : "Capacity for Broker 12"
                  } ]
                }""";

        assertEquals(expectedCapacityConfig, actualCapacityConfig);
    }

    @ParallelTest
    public void testCpuCapacityGeneration() {
        String brokerCpuCapacity = "6.0";
        String brokerCpuCapacityOverride = "2.0";
        String resourceRequestCpu = "3.0";
        String resourceLimitCpu = "4.0";

        Map<String, Storage> storage = Map.of(
            "brokers",
            new JbodStorageBuilder()
                .withVolumes(
                    new PersistentClaimStorageBuilder()
                        .withId(0)
                        .withSize("100Gi")
                        .build()
                )
                .build()
        );

        /* In this test case, when override is set for a broker, it takes precedence over the general broker capacity setting for that
           specific broker, but the general broker capacity takes precedence over the Kafka resource request */
        Map<String, ResourceRequirements> resources = createResources(resourceRequestCpu, resourceLimitCpu);

        BrokerCapacity brokerCapacityOne = new BrokerCapacityBuilder()
                .withCpu(brokerCpuCapacity)
                .withOverrides()
                    .addNewOverride()
                        .addToBrokers(0)
                        .withCpu(brokerCpuCapacityOverride)
                    .endOverride()
                .build();

        verifyBrokerCapacity(storage, resources, brokerCapacityOne, brokerCpuCapacityOverride, brokerCpuCapacity, brokerCpuCapacity);

        // In this test case, when override is set for a broker, it takes precedence over the Kafka resource request
        BrokerCapacity brokerCapacityTwo = new BrokerCapacityBuilder()
                .withOverrides()
                    .addNewOverride()
                        .addToBrokers(0)
                        .withCpu(brokerCpuCapacityOverride)
                    .endOverride()
                .build();

        verifyBrokerCapacity(storage, resources, brokerCapacityTwo, brokerCpuCapacityOverride, resourceRequestCpu, resourceRequestCpu);

        /* In this test case, when neither the override nor the CPU resource request are configured but the CPU
           resource limit for CPU is set for the Kafka brokers; therefore, resource limit will be used as the CPU capacity */
        resources = Map.of("brokers",
                new ResourceRequirementsBuilder()
                        .withLimits(Map.of("cpu", new Quantity(resourceLimitCpu)))
                        .build());

        BrokerCapacity brokerCapacityThree = new BrokerCapacityBuilder().build();

        verifyBrokerCapacity(storage, resources, brokerCapacityThree, resourceLimitCpu, resourceLimitCpu, resourceLimitCpu);

        /* In this test case, when neither the override nor the Kafka resource requests or limits for CPU are configured,
           the CPU capacity will be set to DEFAULT_CPU_CORE_CAPACITY */
        resources = Map.of("brokers", new ResourceRequirementsBuilder().build());

        verifyBrokerCapacity(storage, resources, brokerCapacityThree, CpuCapacity.DEFAULT_CPU_CORE_CAPACITY,
                CpuCapacity.DEFAULT_CPU_CORE_CAPACITY, CpuCapacity.DEFAULT_CPU_CORE_CAPACITY);
    }

    private void verifyBrokerCapacity(Map<String, Storage> storage,
                                      Map<String, ResourceRequirements> resources,
                                      BrokerCapacity brokerCapacity,
                                      String brokerZeroCpuValue,
                                      String brokerOneCpuValue,
                                      String brokerTwoCpuValue) {
        CruiseControlSpec spec = new CruiseControlSpecBuilder()
                .withBrokerCapacity(brokerCapacity)
                .build();

        CapacityConfiguration config = new CapacityConfiguration(Reconciliation.DUMMY_RECONCILIATION, spec, NODES, storage, resources);
        String actualCapacityConfig = config.toJson();
        String expectedCapacityConfig = String.format("""
            {
              "brokerCapacities" : [ {
                "brokerId" : 0,
                "capacity" : {
                  "DISK" : {
                    "/var/lib/kafka/data-0/kafka-log0" : "102400.0"
                  },
                  "CPU" : {
                    "num.cores" : "%s"
                  },
                  "NW_IN" : "10000.0",
                  "NW_OUT" : "10000.0"
                },
                "doc" : "Capacity for Broker 0"
              }, {
                "brokerId" : 1,
                "capacity" : {
                  "DISK" : {
                    "/var/lib/kafka/data-0/kafka-log1" : "102400.0"
                  },
                  "CPU" : {
                    "num.cores" : "%s"
                  },
                  "NW_IN" : "10000.0",
                  "NW_OUT" : "10000.0"
                },
                "doc" : "Capacity for Broker 1"
              }, {
                "brokerId" : 2,
                "capacity" : {
                  "DISK" : {
                    "/var/lib/kafka/data-0/kafka-log2" : "102400.0"
                  },
                  "CPU" : {
                    "num.cores" : "%s"
                  },
                  "NW_IN" : "10000.0",
                  "NW_OUT" : "10000.0"
                },
                "doc" : "Capacity for Broker 2"
              } ]
            }""",
            brokerZeroCpuValue,
            brokerOneCpuValue,
            brokerTwoCpuValue);

        assertEquals(expectedCapacityConfig, actualCapacityConfig);
    }

    private static Map<String, ResourceRequirements> createResources(String cpuRequest, String cpuLimit) {
        return Map.of("brokers",
                new ResourceRequirementsBuilder()
                        .withRequests(Map.of("cpu", new Quantity(cpuRequest)))
                        .withLimits(Map.of("cpu", new Quantity(cpuLimit)))
                        .build());
    }
}
