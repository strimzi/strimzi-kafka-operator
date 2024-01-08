/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.LifecycleBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.strimzi.api.kafka.model.common.ContainerEnvVarBuilder;
import io.strimzi.api.kafka.model.common.Probe;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.ContainerTemplateBuilder;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ContainerUtilsTest {
    @Test
    public void createContainer()   {
        Container cont = ContainerUtils.createContainer(
                "my-name",
                "my-image:latest",
                List.of("/startup.sh", "--port=8443"),
                new SecurityContextBuilder().withRunAsUser(1874L).build(),
                new ResourceRequirementsBuilder().withRequests(Map.of("memory", new Quantity("1Gi"), "cpu", new Quantity("1000m"))).build(),
                List.of(ContainerUtils.createEnvVar("VAR_1", "value1")),
                List.of(ContainerUtils.createContainerPort("my-port", 8443)),
                List.of(VolumeUtils.createVolumeMount("my-volume", "/my-volume")),
                ProbeUtils.execProbe(new Probe(), List.of("/liveness.sh")),
                ProbeUtils.execProbe(new Probe(), List.of("/readiness.sh")),
                null
        );

        assertThat(cont.getName(), is("my-name"));
        assertThat(cont.getImage(), is("my-image:latest"));
        assertThat(cont.getArgs().size(), is(2));
        assertThat(cont.getArgs().get(0), is("/startup.sh"));
        assertThat(cont.getArgs().get(1), is("--port=8443"));
        assertThat(cont.getSecurityContext().getRunAsUser(), is(1874L));
        assertThat(cont.getResources().getRequests().get("memory"), is(new Quantity("1Gi")));
        assertThat(cont.getResources().getRequests().get("cpu"), is(new Quantity("1000m")));
        assertThat(cont.getResources().getLimits(), is(Map.of()));
        assertThat(cont.getEnv().size(), is(1));
        assertThat(cont.getEnv().get(0).getName(), is("VAR_1"));
        assertThat(cont.getEnv().get(0).getValue(), is("value1"));
        assertThat(cont.getPorts().size(), is(1));
        assertThat(cont.getPorts().get(0).getName(), is("my-port"));
        assertThat(cont.getPorts().get(0).getContainerPort(), is(8443));
        assertThat(cont.getVolumeMounts().size(), is(1));
        assertThat(cont.getVolumeMounts().get(0).getName(), is("my-volume"));
        assertThat(cont.getVolumeMounts().get(0).getMountPath(), is("/my-volume"));
        assertThat(cont.getLivenessProbe().getExec().getCommand(), is(List.of("/liveness.sh")));
        assertThat(cont.getReadinessProbe().getExec().getCommand(), is(List.of("/readiness.sh")));
        assertThat(cont.getStartupProbe(), is(nullValue()));
        assertThat(cont.getImagePullPolicy(), is("Always"));
        assertThat(cont.getLifecycle(), is(nullValue()));
    }

    @Test
    public void createContainerWithStartupProbeAndLifecycle()   {
        Container cont = ContainerUtils.createContainer(
                "my-name",
                "my-image:latest",
                List.of("/startup.sh", "--port=8443"),
                new SecurityContextBuilder().withRunAsUser(1874L).build(),
                new ResourceRequirementsBuilder().withRequests(Map.of("memory", new Quantity("1Gi"), "cpu", new Quantity("1000m"))).build(),
                List.of(ContainerUtils.createEnvVar("VAR_1", "value1")),
                List.of(ContainerUtils.createContainerPort("my-port", 8443)),
                List.of(VolumeUtils.createVolumeMount("my-volume", "/my-volume")),
                ProbeUtils.execProbe(new Probe(), List.of("/liveness.sh")),
                ProbeUtils.execProbe(new Probe(), List.of("/readiness.sh")),
                ProbeUtils.execProbe(new Probe(), List.of("/startup.sh")),
                ImagePullPolicy.NEVER,
                new LifecycleBuilder().withNewPreStop().withNewExec().withCommand("/pre-stop.sh").endExec().endPreStop().build()
        );

        assertThat(cont.getName(), is("my-name"));
        assertThat(cont.getImage(), is("my-image:latest"));
        assertThat(cont.getArgs().size(), is(2));
        assertThat(cont.getArgs().get(0), is("/startup.sh"));
        assertThat(cont.getArgs().get(1), is("--port=8443"));
        assertThat(cont.getSecurityContext().getRunAsUser(), is(1874L));
        assertThat(cont.getResources().getRequests().get("memory"), is(new Quantity("1Gi")));
        assertThat(cont.getResources().getRequests().get("cpu"), is(new Quantity("1000m")));
        assertThat(cont.getResources().getLimits(), is(Map.of()));
        assertThat(cont.getEnv().size(), is(1));
        assertThat(cont.getEnv().get(0).getName(), is("VAR_1"));
        assertThat(cont.getEnv().get(0).getValue(), is("value1"));
        assertThat(cont.getPorts().size(), is(1));
        assertThat(cont.getPorts().get(0).getName(), is("my-port"));
        assertThat(cont.getPorts().get(0).getContainerPort(), is(8443));
        assertThat(cont.getVolumeMounts().size(), is(1));
        assertThat(cont.getVolumeMounts().get(0).getName(), is("my-volume"));
        assertThat(cont.getVolumeMounts().get(0).getMountPath(), is("/my-volume"));
        assertThat(cont.getLivenessProbe().getExec().getCommand(), is(List.of("/liveness.sh")));
        assertThat(cont.getReadinessProbe().getExec().getCommand(), is(List.of("/readiness.sh")));
        assertThat(cont.getStartupProbe().getExec().getCommand(), is(List.of("/startup.sh")));
        assertThat(cont.getImagePullPolicy(), is("Never"));
        assertThat(cont.getLifecycle().getPreStop().getExec().getCommand(), is(List.of("/pre-stop.sh")));
    }





    @Test
    public void testCreateContainerPort()   {
        ContainerPort port = ContainerUtils.createContainerPort("my-port", 1874);

        assertThat(port.getName(), is("my-port"));
        assertThat(port.getContainerPort(), is(1874));
        assertThat(port.getProtocol(), is("TCP"));
    }

    @Test
    public void testCreateEnvVar()   {
        EnvVar var = ContainerUtils.createEnvVar("VAR_1", "value1");

        assertThat(var.getName(), is("VAR_1"));
        assertThat(var.getValue(), is("value1"));
    }

    @Test
    public void testCreateEnvVarFromSecret()   {
        EnvVar var = ContainerUtils.createEnvVarFromSecret("VAR_1", "my-secret", "my-key");

        assertThat(var.getName(), is("VAR_1"));
        assertThat(var.getValueFrom().getSecretKeyRef().getName(), is("my-secret"));
        assertThat(var.getValueFrom().getSecretKeyRef().getKey(), is("my-key"));
    }

    @Test
    public void testCreateEnvVarFromFieldRef()   {
        EnvVar var = ContainerUtils.createEnvVarFromFieldRef("VAR_1", "spec.nodeName");

        assertThat(var.getName(), is("VAR_1"));
        assertThat(var.getValueFrom().getFieldRef().getFieldPath(), is("spec.nodeName"));
    }

    @Test
    public void testAddContainerToEnvVarsWithNullTemplate() {
        List<EnvVar> vars = new ArrayList<>();
        vars.add(new EnvVarBuilder().withName("VAR_1").withValue("value1").build());

        ContainerUtils.addContainerEnvsToExistingEnvs(Reconciliation.DUMMY_RECONCILIATION, vars, null);

        assertThat(vars.size(), is(1));
        assertThat(vars.get(0).getName(), is("VAR_1"));
        assertThat(vars.get(0).getValue(), is("value1"));
    }

    @Test
    public void testAddContainerToEnvVarsWithEmptyTemplate() {
        List<EnvVar> vars = new ArrayList<>();
        vars.add(new EnvVarBuilder().withName("VAR_1").withValue("value1").build());

        ContainerUtils.addContainerEnvsToExistingEnvs(Reconciliation.DUMMY_RECONCILIATION, vars, new ContainerTemplate());

        assertThat(vars.size(), is(1));
        assertThat(vars.get(0).getName(), is("VAR_1"));
        assertThat(vars.get(0).getValue(), is("value1"));
    }

    @Test
    public void testAddContainerToEnvVarsWithTemplate() {
        ContainerTemplate template = new ContainerTemplateBuilder()
                .withEnv(new ContainerEnvVarBuilder().withName("VAR_2").withValue("value2").build())
                .build();

        List<EnvVar> vars = new ArrayList<>();
        vars.add(new EnvVarBuilder().withName("VAR_1").withValue("value1").build());

        ContainerUtils.addContainerEnvsToExistingEnvs(Reconciliation.DUMMY_RECONCILIATION, vars, template);

        assertThat(vars.size(), is(2));
        assertThat(vars.get(0).getName(), is("VAR_1"));
        assertThat(vars.get(0).getValue(), is("value1"));
        assertThat(vars.get(1).getName(), is("VAR_2"));
        assertThat(vars.get(1).getValue(), is("value2"));
    }

    @Test
    public void testAddContainerToEnvVarsWithConflict() {
        ContainerTemplate template = new ContainerTemplateBuilder()
                .withEnv(new ContainerEnvVarBuilder().withName("VAR_1").withValue("newValue").build(),
                        new ContainerEnvVarBuilder().withName("VAR_2").withValue("value2").build())
                .build();

        List<EnvVar> vars = new ArrayList<>();
        vars.add(new EnvVarBuilder().withName("VAR_1").withValue("value1").build());

        ContainerUtils.addContainerEnvsToExistingEnvs(Reconciliation.DUMMY_RECONCILIATION, vars, template);

        assertThat(vars.size(), is(2));
        assertThat(vars.get(0).getName(), is("VAR_1"));
        assertThat(vars.get(0).getValue(), is("value1"));
        assertThat(vars.get(1).getName(), is("VAR_2"));
        assertThat(vars.get(1).getValue(), is("value2"));
    }

    @Test
    public void testDetermineImagePullPolicy()  {
        assertThat(ContainerUtils.determineImagePullPolicy(ImagePullPolicy.ALWAYS, "docker.io/repo/image:tag"), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(ContainerUtils.determineImagePullPolicy(ImagePullPolicy.IFNOTPRESENT, "docker.io/repo/image:tag"), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(ContainerUtils.determineImagePullPolicy(ImagePullPolicy.IFNOTPRESENT, "docker.io/repo/image:latest"), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(ContainerUtils.determineImagePullPolicy(ImagePullPolicy.NEVER, "docker.io/repo/image:tag"), is(ImagePullPolicy.NEVER.toString()));
        assertThat(ContainerUtils.determineImagePullPolicy(ImagePullPolicy.NEVER, "docker.io/repo/image:latest-kafka-2.7.0"), is(ImagePullPolicy.NEVER.toString()));
        assertThat(ContainerUtils.determineImagePullPolicy(null, "docker.io/repo/image:latest"), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(ContainerUtils.determineImagePullPolicy(null, "docker.io/repo/image:not-so-latest"), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(ContainerUtils.determineImagePullPolicy(null, "docker.io/repo/image:latest-kafka-2.7.0"), is(ImagePullPolicy.ALWAYS.toString()));
    }

    @Test
    public void testListOrNull()    {
        assertThat(ContainerUtils.listOrNull(null), is(nullValue()));
        assertThat(ContainerUtils.listOrNull(new ContainerBuilder().withName("my-container").build()).size(), is(1));
    }
}
