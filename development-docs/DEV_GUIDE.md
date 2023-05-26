# Development Guide for Strimzi

This document gives a detailed breakdown of the various build processes and options for building Strimzi from source.

<!-- TOC depthFrom:2 -->

- [Developer Quick Start](#developer-quick-start)
- [Build Pre-Requisites](#build-pre-requisites)
- [Using an IDE](#using-an-ide)
   - [IntelliJ IDEA](#intellij-idea)
   - [IDE build problems](#ide-build-problems)
- [Build and deploy Strimzi from source](#build-and-deploy-from-source)
- [Build details](#build-details)
   - [Make targets](#make-targets)
   - [Java versions](#java-versions)
   - [Building Docker images](#building-docker-images)
   - [Tagging and pushing Docker images](#tagging-and-pushing-docker-images)
   - [Docker build options](#docker-build-options)
   - [Build customization](#build-customization)
   - [Local build on Minikube](#local-build-on-minikube)
- [Helm Chart](#helm-chart)
- [Running system tests](#running-system-tests)
- [DCO Signoff](#dco-signoff)
- [Building container images for other platforms with Docker `buildx`](#building-container-images-for-other-platforms-with-docker-buildx)

<!-- /TOC -->

## Developer Quick Start

To build Strimzi from a source you need a Kubernetes or OpenShift cluster available. If you do not have an existing
Kubernetes cluster, you can install [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) to have access
to a cluster on your local machine.

You will also need access to several command line utilities. See the [pre-requisites section](#build-pre-requisites) for
more details.

## Build Pre-Requisites

### Command line tools

To build this project you must first install several command line utilities and a Kubernetes or OpenShift cluster.

- [`make`](https://www.gnu.org/software/make/) - Make build system
- [`mvn`](https://maven.apache.org/index.html) (version 3.5 and above) - Maven CLI
- [`helm`](https://helm.sh/) (version 3) - Helm Package Management System for Kubernetes
- [`asciidoctor` and `asciidoctor-pdf`](https://asciidoctor.org/) - Documentation generation.
    - Use `gem` to install the latest version for your platform.
- [`yq`](https://github.com/mikefarah/yq) - (version 4.2.1 and above) YAML manipulation tool.
    - **Warning:** There are several different `yq` YAML projects in the wild.
      Use [this one](https://github.com/mikefarah/yq). You need version 4.2.1 or above.
- [`docker`](https://docs.docker.com/install/) - Docker command line client
- [`shellcheck`](https://github.com/koalaman/shellcheck) - ShellCheck is a GPLv3 tool that gives warnings and
  suggestions for bash/sh shell scripts.

In order to use `make` these all need to be available on your `$PATH`.

### macOS

The `make` build uses GNU versions of `find`, `sed` and other utilities and is not compatible with the BSD versions
available on macOS. When using macOS, you have to install the GNU versions of `find` and `sed`. When using `brew`, you
can do `brew install gnu-sed findutils grep coreutils`. 
This command will install the GNU versions as `gcp`, `ggrep`, `gsed` and `gfind` and our `make` build will automatically pick them up and use them.

The build requires `bash` version 4+ which is not shipped with macOS but can be installed via homebrew. You can
run `brew install bash` to install a compatible version of `bash`. If you wish to change the default shell to the
updated bash run `sudo bash -c 'echo /usr/local/bin/bash >> /etc/shells'` and `chsh -s /usr/local/bin/bash`

The `mvn` tool might install the latest version of OpenJDK during the brew install. For builds on macOS to succeed,
OpenJDK version 17 needs to be installed. This can be done by running `brew install openjdk@17`. For maven to read the
new Java version, you will need to edit the `~/.mavenrc` file and paste the following
line `export JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home`.

You may come across an issue of linking from the above step. To solve this run this command: 
`sudo ln -sfn /usr/local/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk`.
If this throws an error that it cannot find the file or directory, navigate into `/Library/Java/` (or however deep you
can) and create a new folder named `JavaVirtualMachines` followed by creating a file named `openjdk-17.jdk`. The folder
structure after everything is said and done should look like `/Library/Java/JavaVirtualMachines/openjdk-17.jdk`. After
doing that run the command at the beginning again and this should link the file and allow you to use maven with OpenJDK
version 17.

When running the tests, you may encounter `OpenSSL` related errors for parts that you may not have even worked on, in 
which case you need to make sure you are using `OpenSSL` and not LibreSSL which comes by default with macOS.

### Kubernetes or OpenShift Cluster

In order to run the integration tests and test any changes made to the operators you will need a functioning Kubernetes
or OpenShift cluster. This can be a remote cluster, or a local development cluster.

#### Minikube

The default Minikube setup should allow the integration tests to be run without additional configuration changes.

Sometimes however, updates to Minikube may prevent the test cluster context being correctly set. If you have errors due
to `oc` commands being called on your Minikube cluster then explicitly set the `TEST_CLUSTER` environment variable
before running the `make` commands.

    export TEST_CLUSTER=minikube

## Using an IDE
### IntelliJ IDEA
The project includes a lot of code generation which takes place within the maven build process, that is why just importing the project might end up with a lot of `Cannot resolve symbol` errors.

Follow these steps to import the project in IntelliJ IDEA, run code generation and trigger a reindex in IntelliJ.

1. After cloning the repo, open the folder in IntelliJ IDEA.
2. From the toolbar in the [Maven tool window](https://www.jetbrains.com/help/idea/maven-projects-tool-window.html#toolbar), click on `Generate Sources and Update Folders For All Projects` button to initiate the code generation.
3. Restart the IDE via **Find Action**: press `Ctrl+Shift+A` and type **Restart IDE**.

Afterwards IntelliJ should no longer have any `Cannot resolve symbol` errors.

Note: After running the Maven build in the terminal you might need to [reload the project](https://www.jetbrains.com/help/idea/delegate-build-and-run-actions-to-maven.html#maven_reimport) from the Maven tool window.

### IDE build problems

The build also uses a Java annotation processor. Some IDEs (such as IntelliJ's IDEA) by default don't run the annotation
processor in their build process. You can run `mvn clean install -DskipTests` to run the annotation processor
as part of the `maven` build, and the IDE should then be able to use the generated classes. It is also possible to
configure the IDE to run the annotation processor directly.

Eclipse users may find the [m2e-apt plugin](https://marketplace.eclipse.org/content/m2e-apt) useful for the automatic
configuration of Eclipse projects for annotation processing.

## Build and deploy from source

To build Strimzi from a source the operator and Kafka code needs to be compiled into container images and placed
in a location accessible to the Kubernetes/OpenShift nodes. The easiest way to make your personal Strimzi builds
accessible, is to place them on [Docker Hub](https://hub.docker.com/) or [Quay.io](https://quay.io). The instructions below use Docker Hub,
however other build options (including options for limited or no network access) are available in the sections below
this quick start guide.

1. If you don't have one already, create an account on [Docker Hub](https://hub.docker.com/). Then log your local
   Docker client into Docker Hub using:

        docker login

   This sets Docker Hub as the `docker push` registry target.


2. Make sure that the `DOCKER_ORG` and `DOCKER_REGISTRY` environment variables are set to the same value as your
   username on the Docker Registry, and the Docker Registry you are using.

        export DOCKER_ORG=docker_hub_username
        export DOCKER_REGISTRY=docker_registry_name  #defaults to docker.io if unset

   By default, the `docker_push` target will build the images under the strimzi organisation (
   e.g. `strimzi/operator:latest`) and attempt to push them to the strimzi repositories on the Docker Hub. Only certain
   users are approved to do this, so you should push to your own Docker Hub organisation (account) instead. To do this,
   make sure that the `DOCKER_ORG` and `DOCKER_REGISTRY` environment variables are set to the same value as your
   username on the Docker Registry, and Docker Registry you are using before running the `make` commands.\
   \
   When the Docker images are build, they will be labeled in the
   form: `docker_registry_name/docker_hub_username/operator:latest` in your local repository and pushed to your Docker
   Hub account under the same label.

3. Now build the Docker images and push them to your repository on Docker Hub:

        make clean
        make all

   Once this completes you should have several new repositories under your Docker Hub account:
    - `docker_registry_name/docker_hub_username/kafka`
    - `docker_registry_name/docker_hub_username/kaniko-executor`
    - `docker_registry_name/docker_hub_username/maven-builder`
    - `docker_registry_name/docker_hub_username/operator`

The tests run during the build can be skipped by setting the `MVN_ARGS` environment variable and passing that to the
make command:

        make clean
        make MVN_ARGS='-DskipTests' all

4. To use the newly built images, update
   the `packaging/install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml` to obtain the images from your
   repositories on Docker Registry rather than the official Strimzi images:
   \
   \
   **Linux**
   ```
   sed -Ei -e "s#(image|value): quay.io/strimzi/([a-z0-9-]+):latest#\1: $DOCKER_REGISTRY/$DOCKER_ORG/\2:latest#" \
            -e "s#(image|value): quay.io/strimzi/([a-zA-Z0-9-]+:[0-9.]+)#\1: $DOCKER_REGISTRY/$DOCKER_ORG/\2#" \
            -e "s#([0-9.]+)=quay.io/strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=$DOCKER_REGISTRY/$DOCKER_ORG/\2#" \
            packaging/install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml
    ```

   **macOS**
    ```
    sed -E -i '' -e "s#(image|value): quay.io/strimzi/([a-z0-9-]+):latest#\1: $DOCKER_REGISTRY/$DOCKER_ORG/\2:latest#" \
                 -e "s#(image|value): quay.io/strimzi/([a-zA-Z0-9-]+:[0-9.]+)#\1: $DOCKER_REGISTRY/$DOCKER_ORG/\2#" \
                 -e "s#([0-9.]+)=quay.io/strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=$DOCKER_REGISTRY/$DOCKER_ORG/\2#" \
                  packaging/install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml
    ```

   This updates `060-Deployment-strimzi-cluster-operator.yaml`, replacing all the image references (in `image`
   and `value` properties) with ones with the same name but with the repository changed.
   > *Note*: please ensure you don't commit these changes accidentally.


5. The installation files assume you're installing into the namespace `myproject`. If you wish to use a different one,
   you'll need to replace it in the installation files.
   \
   \
   **Linux**

       sed -Ei "s/myproject/<desired_namespace>/g" packaging/install/cluster-operator/*RoleBinding*.yaml

   **macOS**

        sed -E -i '' -e "s/myproject/<desired_namespace>/g" packaging/install/cluster-operator/*RoleBinding*.yaml

   This updates the RoleBinding and ClusterRoleBinding files to ensure that the roles binding to the service account use
   the correct namespace.
   > *Note*: please ensure you don't commit these changes accidentally.


6. Then deploy the Cluster Operator by running the following (replace `myproject` with your desired namespace if
   necessary):

        # Running against Kubernetes
        kubectl -n myproject create -f packaging/install/cluster-operator

        # Running against OpenShift
        oc -n myproject create -f packaging/install/cluster-operator

7. Finally, you can deploy the cluster custom resource running:

        # Running against Kubernetes
        kubectl -n myproject create -f packaging/examples/kafka/kafka-ephemeral.yaml         

        # Running against OpenShift
        oc -n myproject create -f packaging/examples/kafka/kafka-ephemeral.yaml 

## Build details

### Make targets

Strimzi includes a `Makefile` with various Make targets to build the project.

Commonly used Make targets:

- `docker_build` for [building Docker images](#building-docker-images)
- `docker_tag` for [tagging existing images](#tagging-and-pushing-docker-images)
- `docker_push` for [pushing images to a Docker registry](#tagging-and-pushing-docker-images)

> *Note*: If you are having trouble running any of the Make commands it may help to run `mvn clean` and then `mvn install -DskipTests` before running the commands again.

### Java versions

Strimzi currently developed and tested with Java 17.

### Building Docker images

The `docker_build` target will build the Docker images provided by the Strimzi project. You can build the Docker images
by calling `make docker_build` from the root of the Strimzi repository (this will build the Java code as well). Or you
can build only the Docker images without the Java code by calling `make docker_build` from the `docker-images`
subdirectory (this expects that you have built the Java code before).

The `docker_build` target will **always** build the images under the `strimzi` organization. This is necessary in order
to be able to reuse the base image you might have just built without modifying all Dockerfiles. The `DOCKER_TAG`
environment variable configures the Docker tag to use (default is `latest`).

### Tagging and pushing Docker images

Target `docker_tag` tags the Docker images built by the `docker_build` target. This target is automatically called as
part of the `docker_push` target, but can be called separately if you wish to avoid pushing images to an external
registry.

To configure the `docker_tag` and `docker_push` targets you can set following environment variables:

* `DOCKER_ORG` configures the Docker organization for tagging/pushing the images (defaults to the value of the `$USER`
  environment variable)
* `DOCKER_TAG` configured Docker tag (default is `latest`)
* `DOCKER_REGISTRY` configures the Docker registry where the image will be pushed (default is `docker.io`)

### Docker build options

When building the Docker images you can use an alternative JRE or use an alternate base image.

#### Alternative `docker` command

The build assumes the `docker` command is available on your `$PATH`. You can set the `DOCKER_CMD` environment variable
to use a different `docker` binary or an alternative implementation such as [`podman`](https://podman.io/).

#### Alternative Docker base image

The docker images can be built with an alternative container OS version by adding the environment
variable `ALTERNATE_BASE`. When this environment variable is set, for each component the build will look for a
Dockerfile in the subdirectory named by `ALTERNATE_BASE`. For example, to build docker images based on alpine,
use `ALTERNATE_BASE=alpine make docker_build`. Alternative docker images are an experimental feature not supported by
the core Strimzi team.

### Build customization

The `make all` target can be used to trigger both the `docker_build` and the `docker_push` targets described above. This
will build the Docker images, tag them and push them to the configured repository.

The build can be customised by:

- building fewer [Kafka versions](#kafka-versions)
- customising the [Maven settings](#maven-settings)

Other build options:

- [Local build on Minikube](#local-build-on-minikube)

#### Kafka versions

As part of the Docker image build several versions of Kafka will be built, which can increase build times.
Which Kafka versions are to be built are defined in
the [kafka-versions.yaml](https://github.com/strimzi/strimzi-kafka-operator/blob/main/kafka-versions.yaml) file.
Unwanted versions can be commented out to speed up the build process.

#### Maven Settings

Running `make` invokes Maven for packaging Java based applications (that is, Cluster Operator, Topic Operator, etc).
The `mvn` command can be customized by setting the `MVN_ARGS` environment variable when launching `make all`. For
example:
* `MVN_ARGS=-DskipTests make all` will compile test code, but not run unit or integration tests
* `MVN_ARGS=-DskipITs make all` will compile test code and run unit tests, but not integration tests
* `MVN_ARGS=-Dmaven.test.skip=true make all` won't compile test code and won't run unit or integration tests
the integration tests.

### Local build on Minikube

If you do not want to have the docker daemon running on your local development machine, you can build the container
images in your Minikube VM by setting your docker host to the address of the VM's daemon:

    eval $(minikube docker-env)

The images will then be built and stored in the cluster VM's local image store and then pushed to your configured Docker
registry.

#### Skipping the registry push

You can avoid the `docker_push` step and `sed` commands above by configuring the Docker Host as above and then running:

    make docker_build

    make DOCKER_ORG=strimzi docker_tag

This labels your latest container build as `strimzi/operator:latest` and you can then deploy the standard CRDs without
changing the image targets. However, this will only work if all instances of the `imagePullPolicy:` setting are set
to `IfNotPresent` or `Never`. If not, then the cluster nodes will go to the upstream registry (Docker Hub by default)
and pull the official images instead of using your freshly built image.

## Helm Chart

The `strimzi-kafka-operator` Helm Chart can be installed directly from its source.

    `helm install strimzi-operator packaging/helm-charts/helm3/strimzi-kafka-operator`

The chart is also available in the release artifact as a tarball.

## Running system tests

System tests has its own guide with more information. See [Testing Guide](TESTING.md) document for more information.

## DCO Signoff

The project requires that all commits are signed-off, indicating that _you_ certify the changes with the developer
certificate of origin (DCO) (https://developercertificate.org/). This can be done using `git commit -s` for each commit
in your pull request. Alternatively, to signoff a bunch of commits you can use `git rebase --signoff _your-branch_`.

You can add a commit-msg hook to warn you if the commit you just made locally has not been signed off. Add the following
line to you `.git/hooks/commit-msg` script to print the warning:

```
./tools/git-hooks/signoff-warning-commit-msg $1
```

## Checkstyle pre-commit hook

The Checkstyle plugin runs on all pull requests to the Strimzi repository. If you haven't compiled the code via maven,
before you submit the PR, then formatting bugs can slip through and this can lead to annoying extra pushes to fix
things. In the first instance you should see if your IDE has a Checkstyle plugin that can highlight errors in-line, such
as [this one](https://plugins.jetbrains.com/plugin/1065-checkstyle-idea) for IntelliJ.

You can also run the Checkstyle plugin for every commit you make by adding a pre-commit hook to your local Strimzi git
repository. To do this, add the following line to your `.git/hooks/pre-commit` script, to execute the checks and fail
the commit if errors are detected:

```
./tools/git-hooks/checkstyle-pre-commit
```

## Building container images for other platforms

Docker and Podman supports building images for different platforms.
If you want to build Strimzi images for other platforms, you can set the environment variable `DOCKER_PLATFORM`
to pass additional build options and run the build.
For example, following can be used to build Strimzi images for Linux on Arm64 / AArch64:

```
DOCKER_PLATFORM="--platform linux/arm64" make all
```

## Adding support for new Kafka versions

Following steps are needed to add support for new Kafka releases:
* Edit the `kafka-versions.yaml` file
    * Add a new record with the new Kafka version
    * Make sure only one version is set as default
* If needed, set up the new `kafka-thirdparty-libs` directory
* Run `make all` to regenerate / update all files
* Update the versions in `attributes.adoc` in `documentation/`
* Update all example in `packaging/examples/`