/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.strimzi.api.kafka.model.common.SidecarContainer;
import io.strimzi.api.kafka.model.common.SidecarContainerBuilder;
import io.strimzi.api.kafka.model.common.template.AdditionalVolume;
import io.strimzi.api.kafka.model.common.template.AdditionalVolumeBuilder;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplateBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SidecarUtilsTest {
    
    private static final Reconciliation RECONCILIATION = Reconciliation.DUMMY_RECONCILIATION;

    @Test
    public void testGetReservedContainerNamesKafka() {
        Set<String> reservedNames = SidecarUtils.getReservedContainerNames("kafka");
        assertThat(reservedNames, containsInAnyOrder("kafka", "kafka-init"));
    }

    @Test
    public void testGetReservedContainerNamesConnect() {
        Set<String> reservedNames = SidecarUtils.getReservedContainerNames("connect");
        assertThat(reservedNames, containsInAnyOrder("kafka-connect"));
        
        Set<String> reservedNamesAlt = SidecarUtils.getReservedContainerNames("kafka-connect");
        assertThat(reservedNamesAlt, containsInAnyOrder("kafka-connect"));
    }

    @Test
    public void testGetReservedContainerNamesBridge() {
        Set<String> reservedNames = SidecarUtils.getReservedContainerNames("bridge");
        assertThat(reservedNames, containsInAnyOrder("kafka-bridge"));
        
        Set<String> reservedNamesAlt = SidecarUtils.getReservedContainerNames("kafka-bridge");
        assertThat(reservedNamesAlt, containsInAnyOrder("kafka-bridge"));
    }

    @Test
    public void testGetReservedContainerNamesUnknown() {
        Set<String> reservedNames = SidecarUtils.getReservedContainerNames("unknown");
        assertThat(reservedNames, is(empty()));
    }

    @Test
    public void testGetReservedPortsKafka() {
        Set<Integer> reservedPorts = SidecarUtils.getReservedPorts("kafka");
        assertThat(reservedPorts, containsInAnyOrder(9404, 9999, 9090, 9091, 8443));
    }

    @Test
    public void testGetReservedPortsConnect() {
        Set<Integer> reservedPorts = SidecarUtils.getReservedPorts("connect");
        assertThat(reservedPorts, containsInAnyOrder(9404, 9999, 8083));
    }

    @Test
    public void testGetReservedPortsBridge() {
        Set<Integer> reservedPorts = SidecarUtils.getReservedPorts("bridge");
        assertThat(reservedPorts, containsInAnyOrder(9404, 9999, 8080));
    }

    @Test
    public void testValidSidecarContainers() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("monitoring-sidecar")
                .withImage("monitoring:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8080).withName("metrics").build())
                .build();

        // Should not throw exception
        SidecarUtils.validateSidecarContainers(
                RECONCILIATION,
                List.of(sidecar),
                Set.of(),
                "kafka",
                SidecarUtils.getReservedContainerNames("kafka"),
                SidecarUtils.getReservedPorts("kafka")
        );
    }

    @Test
    public void testInvalidSidecarContainerName() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("kafka") // Reserved name
                .withImage("monitoring:latest")
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar),
                        Set.of(),
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString("'kafka' is reserved by Strimzi"));
    }

    @Test
    public void testInvalidSidecarContainerNameFormat() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("Invalid-Name-With-Uppercase")
                .withImage("monitoring:latest")
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar),
                        Set.of(),
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString("must match regex"));
    }

    @Test
    public void testDuplicateSidecarContainerNames() {
        SidecarContainer sidecar1 = new SidecarContainerBuilder()
                .withName("monitoring")
                .withImage("monitoring:latest")
                .build();

        SidecarContainer sidecar2 = new SidecarContainerBuilder()
                .withName("monitoring") // Duplicate name
                .withImage("logging:latest")
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar1, sidecar2),
                        Set.of(),
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString("already used by another sidecar container"));
    }

    @Test
    public void testInvalidSidecarPortConflict() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("monitoring")
                .withImage("monitoring:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(9090).build()) // Conflicts with Kafka port
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar),
                        Set.of(),
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString("is reserved or already in use"));
    }

    @Test
    public void testMissingContainerName() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withImage("monitoring:latest")
                // No name
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar),
                        Set.of(),
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString("name is required"));
    }

    @Test
    public void testMissingContainerImage() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("monitoring")
                // No image
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar),
                        Set.of(),
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString("image is required"));
    }

    @Test
    public void testValidVolumeReferences() {
        AdditionalVolume volume = new AdditionalVolumeBuilder()
                .withName("config-volume")
                .withConfigMap(new ConfigMapVolumeSourceBuilder().withName("config-map").build())
                .build();

        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("monitoring")
                .withImage("monitoring:latest")
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("config-volume")
                        .withMountPath("/config")
                        .build())
                .build();

        // Should not throw exception
        SidecarUtils.validateVolumeReferences(
                RECONCILIATION,
                List.of(sidecar),
                List.of(volume),
                "kafka"
        );
    }

    @Test
    public void testInvalidVolumeReference() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("monitoring")
                .withImage("monitoring:latest")
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("non-existent-volume")
                        .withMountPath("/config")
                        .build())
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateVolumeReferences(
                        RECONCILIATION,
                        List.of(sidecar),
                        List.of(), // No volumes
                        "kafka"
                )
        );

        assertThat(exception.getMessage(), containsString("references a volume that does not exist"));
    }

    @Test
    public void testExtractPortNumbers() {
        List<ContainerPort> containerPorts = List.of(
                new ContainerPortBuilder().withContainerPort(8080).build(),
                new ContainerPortBuilder().withContainerPort(9090).build(),
                new ContainerPortBuilder().withName("no-port").build() // No port number
        );

        Set<Integer> ports = SidecarUtils.extractPortNumbers(containerPorts);
        assertThat(ports, containsInAnyOrder(8080, 9090));
    }

    @Test
    public void testValidateSidecarContainersWithTemplateValid() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("monitoring")
                .withImage("monitoring:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8080).build())
                .build();

        PodTemplate template = new PodTemplateBuilder()
                .withSidecarContainers(List.of(sidecar))
                .build();

        // Should not throw exception
        SidecarUtils.validateSidecarContainersWithTemplate(
                RECONCILIATION,
                template,
                Set.of(9092), // Kafka user-defined ports
                "kafka"
        );
    }

    @Test
    public void testValidateSidecarContainersWithTemplateInvalid() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("kafka") // Reserved name
                .withImage("monitoring:latest")
                .build();

        PodTemplate template = new PodTemplateBuilder()
                .withSidecarContainers(List.of(sidecar))
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainersWithTemplate(
                        RECONCILIATION,
                        template,
                        Set.of(),
                        "kafka"
                )
        );

        assertThat(exception.getMessage(), containsString("'kafka' is reserved by Strimzi"));
    }

    // Comprehensive tests for all reserved ports
    @ParameterizedTest
    @ValueSource(ints = {9090, 9091, 8443, 9404, 9999})
    public void testReservedPortValidation(int reservedPort) {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("monitoring")
                .withImage("monitoring:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(reservedPort).build())
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar),
                        Set.of(),
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString(reservedPort + " is reserved or already in use"));
    }

    // Interface function integration tests
    @Test
    public void testExtractSidecarContainerPortsFromPodTemplate() {
        SidecarContainer sidecar1 = new SidecarContainerBuilder()
                .withName("metrics")
                .withImage("metrics:latest")
                .withPorts(
                    new ContainerPortBuilder().withContainerPort(8080).build(),
                    new ContainerPortBuilder().withContainerPort(9092).build()
                )
                .build();

        SidecarContainer sidecar2 = new SidecarContainerBuilder()
                .withName("logging")
                .withImage("logging:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8081).build())
                .build();

        PodTemplate template = new PodTemplateBuilder()
                .withSidecarContainers(List.of(sidecar1, sidecar2))
                .build();

        Set<Integer> extractedPorts = SidecarUtils.extractSidecarContainerPorts(template);
        assertThat(extractedPorts, containsInAnyOrder(8080, 9092, 8081));
    }

    @Test
    public void testExtractSidecarContainerPortsFromMultiplePodTemplates() {
        SidecarContainer sidecar1 = new SidecarContainerBuilder()
                .withName("metrics")
                .withImage("metrics:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8080).build())
                .build();

        SidecarContainer sidecar2 = new SidecarContainerBuilder()
                .withName("logging")
                .withImage("logging:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8081).build())
                .build();

        PodTemplate template1 = new PodTemplateBuilder()
                .withSidecarContainers(List.of(sidecar1))
                .build();

        PodTemplate template2 = new PodTemplateBuilder()
                .withSidecarContainers(List.of(sidecar2))
                .build();

        List<SidecarUtils.HasPodTemplate> templates = List.of(
                () -> template1,
                () -> template2
        );

        Set<Integer> extractedPorts = SidecarUtils.extractSidecarContainerPorts(templates);
        assertThat(extractedPorts, containsInAnyOrder(8080, 8081));
    }

    @Test
    public void testExtractSidecarContainerPortsEmpty() {
        PodTemplate template = new PodTemplateBuilder().build(); // No sidecar containers

        Set<Integer> extractedPorts = SidecarUtils.extractSidecarContainerPorts(template);
        assertThat(extractedPorts, is(empty()));
    }

    @Test
    public void testExtractSidecarContainerPortsWithNullPorts() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("no-ports")
                .withImage("no-ports:latest")
                // No ports defined
                .build();

        PodTemplate template = new PodTemplateBuilder()
                .withSidecarContainers(List.of(sidecar))
                .build();

        Set<Integer> extractedPorts = SidecarUtils.extractSidecarContainerPorts(template);
        assertThat(extractedPorts, is(empty()));
    }

    @Test
    public void testExtractSidecarContainerPortsDuplicatesRemoved() {
        SidecarContainer sidecar1 = new SidecarContainerBuilder()
                .withName("metrics1")
                .withImage("metrics:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8080).build())
                .build();

        SidecarContainer sidecar2 = new SidecarContainerBuilder()
                .withName("metrics2")
                .withImage("metrics:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8080).build()) // Same port
                .build();

        PodTemplate template = new PodTemplateBuilder()
                .withSidecarContainers(List.of(sidecar1, sidecar2))
                .build();

        Set<Integer> extractedPorts = SidecarUtils.extractSidecarContainerPorts(template);
        assertThat(extractedPorts, hasSize(1));
        assertThat(extractedPorts, containsInAnyOrder(8080));
    }

    // User-defined port conflicts
    @Test
    public void testUserDefinedPortConflict() {
        SidecarContainer sidecar = new SidecarContainerBuilder()
                .withName("monitoring")
                .withImage("monitoring:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8082).build())
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar),
                        Set.of(8082), // User-defined port in use
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString("8082 is reserved or already in use"));
    }

    @Test
    public void testMultipleSidecarPortConflicts() {
        SidecarContainer sidecar1 = new SidecarContainerBuilder()
                .withName("sidecar1")
                .withImage("sidecar1:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8083).build())
                .build();

        SidecarContainer sidecar2 = new SidecarContainerBuilder()
                .withName("sidecar2")
                .withImage("sidecar2:latest")
                .withPorts(new ContainerPortBuilder().withContainerPort(8083).build()) // Same port as sidecar1
                .build();

        InvalidResourceException exception = assertThrows(InvalidResourceException.class, () ->
                SidecarUtils.validateSidecarContainers(
                        RECONCILIATION,
                        List.of(sidecar1, sidecar2),
                        Set.of(),
                        "kafka",
                        SidecarUtils.getReservedContainerNames("kafka"),
                        SidecarUtils.getReservedPorts("kafka")
                )
        );

        assertThat(exception.getMessage(), containsString("8083 is reserved or already in use"));
    }
}