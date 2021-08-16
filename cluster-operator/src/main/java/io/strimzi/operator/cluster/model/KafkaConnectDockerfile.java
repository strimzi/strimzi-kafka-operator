/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.connect.build.Artifact;
import io.strimzi.api.kafka.model.connect.build.Build;
import io.strimzi.api.kafka.model.connect.build.DownloadableArtifact;
import io.strimzi.api.kafka.model.connect.build.JarArtifact;
import io.strimzi.api.kafka.model.connect.build.MavenArtifact;
import io.strimzi.api.kafka.model.connect.build.OtherArtifact;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.TgzArtifact;
import io.strimzi.api.kafka.model.connect.build.ZipArtifact;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class is used to generate the Dockerfile used by Kafka Connect Build. It takes the API definition with the
 * desired plugins and generates a Dockerfile which pulls and installs them. To generate the Dockerfile, it is using
 * the PrintWriter.
 */
public class KafkaConnectDockerfile {
    private static final String BASE_PLUGIN_PATH = "/opt/kafka/plugins/";
    private static final String ROOT_USER = "root:root";
    private static final String NON_PRIVILEGED_USER = "1001";

    private static final String ENV_VAR_HTTP_PROXY = "HTTP_PROXY";
    private static final String ENV_VAR_HTTPS_PROXY = "HTTPS_PROXY";
    private static final String ENV_VAR_NO_PROXY = "NO_PROXY";

    private static final String HTTP_PROXY = System.getenv(ENV_VAR_HTTP_PROXY);
    private static final String HTTPS_PROXY = System.getenv(ENV_VAR_HTTPS_PROXY);
    private static final String NO_PROXY = System.getenv(ENV_VAR_NO_PROXY);

    private final String dockerfile;

    private final Pattern special = Pattern.compile("[!@#$%&*()+=\\\\|<>?{};\\[\\]~]");
    private static final String DEFAULT_MAVEN_IMAGE = "registry.access.redhat.com/ubi8/openjdk-11:1.3-18";
    private final String mavenImage;

    /**
     * Broker configuration template constructor
     *
     * @param fromImage     Image which should be used as a base image in the FROM statement
     * @param connectBuild  The Build definition from the API
     */
    public KafkaConnectDockerfile(String fromImage, Build connectBuild) {
        this.mavenImage = System.getenv().getOrDefault(ClusterOperatorConfig.STRIMZI_DEFAULT_MAVEN_IMAGE, DEFAULT_MAVEN_IMAGE);
        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);

        printHeader(writer); // Print initial comment
        connectorPluginsPreStage(writer, connectBuild.getPlugins());
        from(writer, fromImage); // Create FROM statement
        user(writer, ROOT_USER); // Switch to root user to be able to add plugins
        proxy(writer); // Configures proxy environment variables
        connectorPlugins(writer, connectBuild.getPlugins());
        user(writer, NON_PRIVILEGED_USER); // Switch back to the regular unprivileged user

        dockerfile = stringWriter.toString();

        writer.close();
    }

    /**
     * Generates initial stage for multi-stage build
     * @param writer        Writer for printing the Docker commands
     * @param plugins       List of plugins which should be added to the container image
     */
    private void connectorPluginsPreStage(PrintWriter writer, List<Plugin> plugins) {
        List<Artifact> mvnPlugins = plugins.stream().flatMap(plugin -> plugin.getArtifacts().stream().filter(artifact -> artifact instanceof MavenArtifact)).collect(Collectors.toList());
        if (mvnPlugins.size() > 0) {
            writer.println("FROM " + mavenImage + " AS downloadArtifacts");
            mvnPlugins.forEach(mvn -> {
                checkForShellMetachars(((MavenArtifact) mvn).getGroup());
                checkForShellMetachars(((MavenArtifact) mvn).getArtifact());
                checkForShellMetachars(((MavenArtifact) mvn).getVersion());
                String repo = ((MavenArtifact) mvn).getRepository() == null ? MavenArtifact.DEFAULT_REPOSITORY : maybePatchRepository(((MavenArtifact) mvn).getRepository());
                String artifactHash = Util.sha1Prefix(((MavenArtifact) mvn).getGroup() + ((MavenArtifact) mvn).getArtifact() + ((MavenArtifact) mvn).getVersion());
                String downloadPomCmd = String.format("curl -L --create-dirs --output /tmp/%s/pom.xml %s%s/%s/%s/%s-%s.pom",
                        artifactHash,
                        repo,
                        ((MavenArtifact) mvn).getGroup().replace(".", "/"), //org.apache.camel is translated as org/apache/camel in the URL
                        ((MavenArtifact) mvn).getArtifact().replace(".", "/"),
                        ((MavenArtifact) mvn).getVersion(),
                        ((MavenArtifact) mvn).getArtifact(),
                        ((MavenArtifact) mvn).getVersion());

                writer.println("RUN " + downloadPomCmd + " \\");
                writer.println("      && mvn dependency:copy-dependencies -DoutputDirectory=/tmp/artifacts/" + artifactHash + " -f /tmp/" + artifactHash + "/pom.xml");
                writer.println();
            });
        }
    }

    private void checkForShellMetachars(String toBeChecked) {
        Matcher hasSpecial = special.matcher(toBeChecked);
        if (hasSpecial.find()) {
            throw new InvalidConfigurationException("String '" + toBeChecked + "' contains forbidden character");
        }
    }

    /**
     * Generates the FROM statement to the Dockerfile. It uses the image passes in the parameter as the base image.
     *
     * @param writer        Writer for printing the Docker commands
     * @param fromImage     Base image which should be used
     */
    private void from(PrintWriter writer, String fromImage) {
        writer.println("FROM " + fromImage);
        writer.println();
    }

    /**
     * Generates proxy arguments if set in the operator
     *
     * @param writer        Writer for printing the Docker commands
     */
    private void proxy(PrintWriter writer) {
        if (HTTP_PROXY != null) {
            writer.println(String.format("ARG %s=%s", ENV_VAR_HTTP_PROXY.toLowerCase(Locale.ENGLISH), HTTP_PROXY));
            writer.println();
        }

        if (HTTPS_PROXY != null) {
            writer.println(String.format("ARG %s=%s", ENV_VAR_HTTPS_PROXY.toLowerCase(Locale.ENGLISH), HTTPS_PROXY));
            writer.println();
        }

        if (NO_PROXY != null) {
            writer.println(String.format("ARG %s=%s", ENV_VAR_NO_PROXY.toLowerCase(Locale.ENGLISH), NO_PROXY));
            writer.println();
        }
    }

    /**
     * Generates the USER statement in the Dockerfile to switch the user under which the next commands will be running.
     *
     * @param writer    Writer for printing the Docker commands
     * @param user      User to which the Dockefile should switch
     */
    private void user(PrintWriter writer, String user) {
        writer.println("USER " + user);
        writer.println();
    }

    /**
     * Adds the commands to donwload and possibly unpact the connector plugins
     *
     * @param writer    Writer for printing the Docker commands
     * @param plugins   List of plugins which should be added to the container image
     */
    private void connectorPlugins(PrintWriter writer, List<Plugin> plugins) {
        for (Plugin plugin : plugins)   {
            addPlugin(writer, plugin);
        }
    }

    /**
     * Adds a particular connector plugin to the container image. It will go through the individual artifacts and add
     * them one by one depending on their type.
     *
     * @param writer    Writer for printing the Docker commands
     * @param plugin    A single plugin which should be added to the new container image
     */
    private void addPlugin(PrintWriter writer, Plugin plugin)    {
        printSectionHeader(writer, "Connector plugin " + plugin.getName());

        String connectorPath = BASE_PLUGIN_PATH + plugin.getName();

        for (Artifact art : plugin.getArtifacts())  {
            if (art instanceof JarArtifact) {
                addJarArtifact(writer, connectorPath, (JarArtifact) art);
            } else if (art instanceof TgzArtifact) {
                addTgzArtifact(writer, connectorPath, (TgzArtifact) art);
            } else if (art instanceof ZipArtifact) {
                addZipArtifact(writer, connectorPath, (ZipArtifact) art);
            } else if (art instanceof MavenArtifact) {
                addMavenArtifact(writer, connectorPath, (MavenArtifact) art);
            } else if (art instanceof OtherArtifact) {
                addOtherArtifact(writer, connectorPath, (OtherArtifact) art);
            } else {
                throw new RuntimeException("Unexpected artifact type " + art.getType());
            }
        }
    }

    private void validateUrlPresence(DownloadableArtifact art) {
        if (art.getUrl() == null) {
            throw new InvalidConfigurationException(art.getType() + " artifact is missing a URL.");
        }
    }

    private void validateGavPresence(MavenArtifact art) {
        if (art.getGroup() == null) {
            throw new InvalidConfigurationException(art.getType() + " artifact is missing a group ID.");
        }
        if (art.getArtifact() == null) {
            throw new InvalidConfigurationException(art.getType() + " artifact is missing an artifact ID.");
        }
        if (art.getVersion() == null) {
            throw new InvalidConfigurationException(art.getType() + " artifact is missing a version number.");
        }
    }

    /**
     * Add command sequence for downloading files and checking their checksums.
     *
     * @param writer            Writer for printing the Docker commands
     * @param connectorPath     Path where the connector to which this artifact belongs should be downloaded
     * @param jar               The JAR-type artifact
     */
    private void addJarArtifact(PrintWriter writer, String connectorPath, JarArtifact jar) {
        validateUrlPresence(jar);
        String artifactHash = Util.sha1Prefix(jar.getUrl());
        String artifactDir = connectorPath + "/" + artifactHash;
        String artifactPath = artifactDir + "/" + artifactHash + ".jar";
        String downloadCmd =  "curl -L --output " + artifactPath + " " + jar.getUrl();

        addUnmodifiedArtifact(writer, jar, artifactDir, downloadCmd, artifactPath);
    }

    /**
     * Add command sequence for downloading files and checking their checksums.
     *
     * @param writer            Writer for printing the Docker commands
     * @param connectorPath     Path where the connector to which this artifact belongs should be downloaded
     * @param other             The Other-type artifact
     */
    private void addOtherArtifact(PrintWriter writer, String connectorPath, OtherArtifact other) {
        String artifactHash = Util.sha1Prefix(other.getUrl());
        String artifactDir = connectorPath + "/" + artifactHash;
        String fileName = other.getFileName() != null ? other.getFileName() : artifactHash;
        String artifactPath = artifactDir + "/" + fileName;
        String downloadCmd =  "curl -L --output " + artifactPath + " " + other.getUrl();

        addUnmodifiedArtifact(writer, other, artifactDir, downloadCmd, artifactPath);
    }

    /**
     * Adds download command for artifacts which are just downloaded without any unpacking or other processing.
     *
     * @param writer            Writer for printing the Docker commands
     * @param art               Artifact which should be downloaded
     * @param artifactDir       Directory into which the artifact should be downloaded
     * @param downloadCmd       Command for downloading the artifact
     * @param artifactPath      Full path of the artifact
     */
    private void addUnmodifiedArtifact(PrintWriter writer, DownloadableArtifact art, String artifactDir, String downloadCmd, String artifactPath)    {
        writer.println("RUN mkdir -p " + artifactDir + " \\");

        if (art.getSha512sum() == null || art.getSha512sum().isEmpty()) {
            // No checksum => we just download the file
            writer.println("      && " + downloadCmd);
        } else {
            // Checksum exists => we need to check it
            String checksum = art.getSha512sum() + " " + artifactPath;

            writer.println("      && " + downloadCmd + " \\");
            writer.println("      && echo \"" + checksum + "\" > " + artifactPath + ".sha512 \\");
            writer.println("      && sha512sum --check " + artifactPath + ".sha512 \\");
            writer.println("      && rm -f " + artifactPath + ".sha512");
        }

        writer.println();
    }

    /**
     * Add command sequence for downloading and unpacking TAR.GZ archives and checking their checksums.
     *
     * @param writer            Writer for printing the Docker commands
     * @param connectorPath     Path where the connector to which this artifact belongs should be downloaded
     * @param tgz               The TGZ-type artifact
     */
    private void addTgzArtifact(PrintWriter writer, String connectorPath, TgzArtifact tgz) {
        validateUrlPresence(tgz);
        String artifactHash = Util.sha1Prefix(tgz.getUrl());
        String artifactDir = connectorPath + "/" + artifactHash;
        String archivePath = connectorPath + "/" + artifactHash + ".tgz";

        String downloadCmd =  "curl -L --output " + archivePath + " " + tgz.getUrl();
        String unpackCmd =  "tar xvfz " + archivePath + " -C " + artifactDir;
        String deleteCmd =  "rm -vf " + archivePath;

        writer.println("RUN mkdir -p " + artifactDir + " \\");

        if (tgz.getSha512sum() == null || tgz.getSha512sum().isEmpty()) {
            // No checksum => we just download and unpack the file
            writer.println("      && " + downloadCmd + " \\");
            writer.println("      && " + unpackCmd + " \\");
            writer.println("      && " + deleteCmd);
        } else {
            // Checksum exists => we need to check it
            String checksum = tgz.getSha512sum() + " " + archivePath;

            writer.println("      && " + downloadCmd + " \\");
            writer.println("      && echo \"" + checksum + "\" > " + archivePath + ".sha512 \\");
            writer.println("      && sha512sum --check " + archivePath + ".sha512 \\");
            writer.println("      && rm -f " + archivePath + ".sha512 \\");
            writer.println("      && " + unpackCmd + " \\");
            writer.println("      && " + deleteCmd);
        }

        writer.println();
    }

    /**
     * Add command sequence for downloading and unpacking TAR.ZIP archives and checking their checksums.
     *
     * @param writer            Writer for printing the Docker commands
     * @param connectorPath     Path where the connector to which this artifact belongs should be downloaded
     * @param zip               The ZIP-type artifact
     */
    private void addZipArtifact(PrintWriter writer, String connectorPath, ZipArtifact zip) {
        validateUrlPresence(zip);
        String artifactHash = Util.sha1Prefix(zip.getUrl());
        String artifactDir = connectorPath + "/" + artifactHash;
        String archivePath = connectorPath + "/" + artifactHash + ".zip";

        String downloadCmd =  "curl -L --output " + archivePath + " " + zip.getUrl();
        String unpackCmd =  "unzip " + archivePath + " -d " + artifactDir;
        String deleteSymLinks = "find " + artifactDir + " -type l | xargs rm -f";
        String deleteCmd =  "rm -vf " + archivePath;

        writer.println("RUN mkdir -p " + artifactDir + " \\");

        if (zip.getSha512sum() == null || zip.getSha512sum().isEmpty()) {
            // No checksum => we just download and unpack the file
            writer.println("      && " + downloadCmd + " \\");
            writer.println("      && " + unpackCmd + " \\");
            writer.println("      && " + deleteSymLinks + " \\");
            writer.println("      && " + deleteCmd);
        } else {
            // Checksum exists => we need to check it
            String checksum = zip.getSha512sum() + " " + archivePath;

            writer.println("      && " + downloadCmd + " \\");
            writer.println("      && echo \"" + checksum + "\" > " + archivePath + ".sha512 \\");
            writer.println("      && sha512sum --check " + archivePath + ".sha512 \\");
            writer.println("      && rm -f " + archivePath + ".sha512 \\");
            writer.println("      && " + unpackCmd + " \\");
            writer.println("      && " + deleteSymLinks + " \\");
            writer.println("      && " + deleteCmd);
        }

        writer.println();
    }

    /**
     * Add command sequence for downloading Maven artifact
     *
     * @param writer            Writer for printing the Docker commands
     * @param connectorPath     Path where the connector to which this artifact belongs should be downloaded
     * @param mvn               The maven artifact
     */
    private void addMavenArtifact(PrintWriter writer, String connectorPath, MavenArtifact mvn) {
        validateGavPresence(mvn);
        String repo = mvn.getRepository() == null ? MavenArtifact.DEFAULT_REPOSITORY : maybePatchRepository(mvn.getRepository());
        String artifactHash = Util.sha1Prefix(mvn.getGroup() + mvn.getArtifact() + mvn.getVersion());
        String artifactDir = connectorPath + "/" + artifactHash;

        String downloadJarCmd = String.format("curl -L --create-dirs --output %s/%s-%s.jar %s%s/%s/%s/%s-%s.jar",
                artifactDir,
                mvn.getArtifact(),
                mvn.getVersion(),
                repo,
                mvn.getGroup().replace(".", "/"), //org.apache.camel is translated as org/apache/camel in the URL
                mvn.getArtifact().replace(".", "/"),
                mvn.getVersion(),
                mvn.getArtifact(),
                mvn.getVersion());

        writer.println("RUN " + downloadJarCmd);
        writer.println("COPY --from=downloadArtifacts /tmp/artifacts/" + artifactHash + " " + artifactDir);
        writer.println();
    }

    /**
     * @param repository The repository to check whether contains the slash as the last character
     * @return The repository with slash ('/') as the last character
     */
    private String maybePatchRepository(String repository) {
        if (repository.lastIndexOf('/') + 1 == repository.length()) {
            return repository;
        } else {
            return repository + "/";
        }
    }

    /**
     * Internal method which prints the section header into the Dockerfile. This makes it more human readable.
     *
     * @param sectionName   Name of the section for which is this header printed
     */
    private void printSectionHeader(PrintWriter writer, String sectionName)   {
        writer.println("##########");
        writer.println("# " + sectionName);
        writer.println("##########");
    }

    /**
     * Prints the file header which is on the beginning of the Dockerfile.
     */
    private void printHeader(PrintWriter writer)   {
        writer.println("##############################");
        writer.println("##############################");
        writer.println("# This file is automatically generated by the Strimzi Cluster Operator");
        writer.println("# Any changes to this file will be ignored and overwritten!");
        writer.println("##############################");
        writer.println("##############################");
        writer.println();
    }

    /**
     * Returns the generated Dockerfile for building new Kafka Connect image with additional connectors.
     *
     * @return  Dockerfile
     */
    public String getDockerfile() {
        return dockerfile;
    }

    /**
     * Returns the hash stub identifying the Dockerfile. This can be used to detect changes.
     *
     * @return  Dockerfile hash stub
     */
    public String hashStub()    {
        return Util.sha1Prefix(dockerfile);
    }
}
