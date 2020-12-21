# Development Guide for Strimzi

This document gives a detailed breakdown of the various build processes and options for building Strimzi from source.

<!-- TOC depthFrom:2 -->

- [Developer Quick Start](#developer-quick-start)
- [Build Pre-requisites](#build-pre-requisites)
- [Make targets](#make-targets)
- [Docker build options](#docker-build-options)
- [Building Strimzi](#building-strimzi)
- [Helm Chart](#helm-chart)
- [Running system tests](#running-system-tests)
- [DCO Signoff](#cdo-signoff)
- [IDE build problems](#ide-build-problems)
- [Building container images for other platforms with Docker `buildx`](#building-container-images-for-other-platforms-with-docker-buildx)

<!-- /TOC -->

## Developer Quick Start

To build Strimzi from source you need an Kubernetes or OpenShift cluster available. You can install either [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) or [minishift](https://www.okd.io/minishift/) to have access to a cluster on your local machine.

You will also need access to several command line utilities. See the [pre-requisites section](#build-pre-requisites) for more details. Make sure your minishift user has `cluster-admin` role - see [minishift](#Minishift) setup details.


## Build from source

To build Strimzi from source the operator and Kafka code needs to be compiled into Docker container images and placed in a location accessible to the Kubernetes/OpenShift nodes. The easiest way to make your personal Strimzi builds accessible, is to place them on the [Docker Hub](https://hub.docker.com/). The instructions below use this method, however other build options (including options for limited or no network access) are available in the sections below this quick start guide. The commands below should work for both minishift and minikube clusters:

1. If you don't have one already, create an account on the [Docker Hub](https://hub.docker.com/). Then log your local Docker client into the Hub using:

        docker login

2. Make sure that the `DOCKER_ORG` environment variable is set to the same value as your username on Docker Hub.

        export DOCKER_ORG=docker_hub_username

3. Now build the Docker images and push them to your repository on Docker Hub:

        make clean
        make all

   Once this completes you should have several new repositories under your Docker Hub account (`docker_hub_username/operator`, `docker_hub_username/kafka` and `docker_hub_username/test-client`).

   The tests run during the build can be skipped by setting the `MVN_ARGS` environment variable and passing that to the make command:

        make clean
        make MVN_ARGS='-DskipTests -DskipIT' all

4. To use the newly built images, update the `install/cluster-operator/060-Deployment-strimzi-cluster-operator.yml` to obtain the images from your repositories on Docker Hub rather than the official Strimzi images:

    ```
    sed -Ei -e "s#(image|value): strimzi/([a-z0-9-]+):latest#\1: $DOCKER_ORG/\2:latest#" \
            -e "s#([0-9.]+)=strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=$DOCKER_ORG/\2#" \
            install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml
    ```

   This updates `060-Deployment-strimzi-cluster-operator.yaml`, replacing all the image references (in `image` and `value` properties) with ones with the same name but with the repository changed.

5. Then you can deploy the Cluster Operator by running: 

   For a minikube cluster:

        kubectl create -f install/cluster-operator
   
   Or for a minishift cluster:
        
        oc create -f install/cluster-operator


6. Finally, you can deploy the cluster custom resource running:
   
   For minikube cluster:

        kubectl create -f examples/kafka/kafka-ephemeral.yaml 
   
   Or for a minishift cluster:

        oc create -f examples/kafka/kafka-ephemeral.yaml


## Build Pre-Requisites

### Command line tools

To build this project you must first install several command line utilities and a Kubernetes or OpenShift cluster.

- [`make`](https://www.gnu.org/software/make/) - Make build system
- [`mvn`](https://maven.apache.org/index.html) (version 3.5 and above) - Maven CLI
- [`helm`](https://helm.sh/) - Helm Package Management System for Kubernetes
    - Both Helm 2 and Helm 3 are supported. In order to use either two, you need to download both versions. A convenient way to do so is to download the [get_helm.sh](https://helm.sh/docs/intro/install/#from-script) script and specify the version you want to download either for Helm 2 or Helm 3.
    - After you download one binary, you can, for convenience, rename it to `helm2` and `helm3` accordingly.
    - Note that if you are using the Helm version 2 charts, after installing the Helm CLI, ensure you run `helm2 init` to configure to your cluster.
- [`asciidoctor` and `asciidoctor-pdf`](https://asciidoctor.org/) - Documentation generation. 
    - Use `gem` to install latest version for your platform.
- [`yq`](https://github.com/mikefarah/yq) - (version 3.3.1 and above) YAML manipulation tool. 
    - **Warning:** There are several different `yq` YAML projects in the wild. Use [this one](https://github.com/mikefarah/yq). You need **v3** version.
- [`docker`](https://docs.docker.com/install/) - Docker command line client

In order to use `make` these all need to be available on your `$PATH`.

### Mac OS

The `make` build is using GNU versions of `find` and `sed` utilities and is not compatible with the BSD versions available on Mac OS. When using Mac OS, you have to install the GNU versions of `find` and `sed`. When using `brew`, you can do `brew install gnu-sed findutils grep coreutils`. This command will install the GNU versions as `gcp`, `ggrep`, `gsed` and `gfind` and our `make` build will automatically pick them up and use them.

The build requires `bash` version 4+ which is not shipped Mac OS but can be installed via homebrew. You can run `brew install bash` to install a compatible version of `bash`. If you wish to change the default shell to the updated bash run `sudo bash -c 'echo /usr/local/bin/bash >> /etc/shells'` and `chsh -s /usr/local/bin/bash`

#### Install Minishift (local install of OpenShift)

`brew cask install minishift`

At the moment of this writing you will need to fix xhyve driver for Minishift to work:
https://stackoverflow.com/questions/56358247/error-creating-new-host-json-cannot-unmarshal-bool-into-go-struct-field-driver
```
brew uninstall docker-machine-driver-xhyve
brew edit docker-machine-driver-xhyve
```

Change tag and revision:
`:tag => "v0.3.3", :revision => "7d92f74a8b9825e55ee5088b8bfa93b042badc47"`

```
brew install docker-machine-driver-xhyve
sudo chown root:wheel $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
sudo chmod u+s $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
```

### Kubernetes or OpenShift Cluster

In order to run the integration tests and test any changes made to the operators you will need a functioning Kubernetes or OpenShift cluster. This can be a remote cluster or a local development cluster.

#### Minishift 

In order to perform the operations necessary for the integration tests, your user must have the cluster administrator role assigned. For example, if your username is `developer`, you can add the `cluster-admin` role using the commands below:

    oc login -u system:admin

    oc adm policy add-cluster-role-to-user cluster-admin developer
    
    oc login -u developer -p <password>

#### Minikube

The default minishift setup should allow the integration tests to be run without additional configuration changes. 

Sometimes however, updates to minikube may prevent the test cluster context being correctly set. If you have errors due to `oc` commands being called on your minikube cluster then explicitly set the `TEST_CLUSTER` environment variable before running the `make` commands.

    export TEST_CLUSTER=minikube
                                                                                            
## Make targets

Strimzi includes a `Makefile` with various Make targets to build the project.

Commonly used Make targets:

 - `docker_build` for [building Docker images](#building-docker-images)
 - `docker_tag` for [tagging existing images](#tagging-and-pushing-docker-images)
 - `docker_push` for [pushing images to a Docker registry](#tagging-and-pushing-docker-images)

>*Note*: If you are having trouble running any of the Make commands it may help to run `mvn clean` and then `mvn install -DskipTests -DskipITs` before running the commands again.

### Java versions

To use different Java version for the Maven build, you can specify the environment variable `JAVA_VERSION_BUILD` and set it to the desired Java version.
For example, for building with Java 8, you can use `export JAVA_VERSION_BUILD=1.8.0` or for Java 11 you can use `export JAVA_VERSION_BUILD=11`. 

### Building Docker images

The `docker_build` target will build the Docker images provided by the Strimzi project. You can build all Strimzi Docker images by calling `make docker_build` from the root of the Strimzi repository. Or you can build an individual Docker image by running `make docker_build` from the subdirectories with their respective Dockerfiles - e.g. `kafka_base`, `kafka` etc.

The `docker_build` target will **always** build the images under the `strimzi` organization. This is necessary in order to be able to reuse the base image you might have just built without modifying all Dockerfiles. The `DOCKER_TAG` environment variable configures the Docker tag to use (default is `latest`).

### Tagging and pushing Docker images

Target `docker_tag` tags the Docker images built by the `docker_build` target. This target is automatically called as part of the `docker_push` target, but can be called separately if you wish to avoid pushing images to an external registry.

To configure the `docker_tag` and `docker_push` targets you can set following environment variables:
* `DOCKER_ORG` configures the Docker organization for tagging/pushing the images (defaults to the value of the `$USER` environment variable)
* `DOCKER_TAG` configured Docker tag (default is `latest`)
* `DOCKER_REGISTRY` configures the Docker registry where the image will be pushed (default is `docker.io`)

## Docker build options

When building the Docker images you can use an alternative JRE or use an alternate base image.

### Alternative Docker image JRE

The docker images can be built with an alternative Java version by setting the environment variable `JAVA_VERSION`. 
For example, to build docker images that have the Java 8 JRE installed use `JAVA_VERSION=1.8.0 make docker_build`.  If not present, the container images will use Java **11** by default.

### Alternative `docker` command

The build assumes the `docker` command is available on your `$PATH`. You can set the `DOCKER_CMD` environment variable to use a different `docker` binary or an alternative implementation such as [`podman`](https://podman.io/).

### Alternative Docker base image

The docker images can be built with an alternative container OS version by adding the environment variable `ALTERNATE_BASE`.  When this environment variable is set, for each component the build will look for a Dockerfile in the subdirectory named by `ALTERNATE_BASE`.  For example, to build docker images based on alpine, use `ALTERNATE_BASE=alpine make docker_build`.  Alternative docker images are an experimental feature not supported by the core Strimzi team.


## Building Strimzi

The `make all` target can be used to trigger both the `docker_build` and the `docker_push` targets described above. This will build the Docker images, tag them and push them to the configured repository.

The build can be customised by:

 - building fewer [Kafka versions](#kafka-versions)
 - customising the [Maven settings](#maven-settings)

Other build options:

 - [Local build with push to Docker Hub](#local-build-with-push-to-docker-hub)
 - [Local build with push to Minishift Docker registry](#local-build-with-push-to-minishift-docker-registry)
 - [Local build on Minishift or Minikube](#local-build-on-minishift-or-minikube)

### Kafka versions

As part of the Docker image build several different versions of Kafka will be built, which can increase build times. Which Kafka versions are to be built are defined in the [kafka-versions.yaml](https://github.com/strimzi/strimzi-kafka-operator/blob/master/kafka-versions.yaml) file. Unwanted versions can be commented out to speed up the build process. 

### Maven Settings

Running `make` invokes Maven for packaging Java based applications (that is, Cluster Operator, Topic Operator, etc). The `mvn` command can be customized by setting the `MVN_ARGS` environment variable when launching `make all`. For example, `MVN_ARGS=-DskipTests make all` can be used to avoid running the unit tests and adding `-DskipIT` will skip the integration tests.

### Local build with push to Docker Hub

To build the images on your local machine and push them to the Docker Hub, log into Docker Hub with your account details. This sets the Hub as the `docker push` registry target.

    docker login

By default the `docker_push` target will build the images under the strimzi organisation (e.g. `strimzi/operator:latest`) and attempt to push them to the strimzi repositories on the Docker Hub. Only certain users are approved to do this so you should push to your own Docker Hub organisation (account) instead. To do this, make sure that the `DOCKER_ORG` environment variable is set to the same value as your username on Docker Hub before running the `make` commands.

    export DOCKER_ORG=docker_hub_username

When the Docker images are build, they will be labeled in the form: `docker_hub_username/operator:latest` in your local repository and pushed to your Docker Hub account under the same label.

To use these newly built images, update the `install/cluster-operator/060-Deployment-strimzi-cluster-operator.yml` to obtain the images from your repositories on Docker Hub rather than the official Strimzi images, replacing `docker_hub_username` with the relevant value:

```
sed -Ei -e 's#(image|value): strimzi/([a-z0-9-]+):latest#\1: docker_hub_username/\2:latest#' \
        -e 's#([0-9.]+)=strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=docker_hub_username/\2#' \
        install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml
```

This updates `060-Deployment-strimzi-cluster-operator.yaml`, replacing all the image references (in `image` and `value` properties) with ones with the same name but with the repository changed.

Then you can deploy the Cluster Operator by running (for an OpenShift cluster):

    oc create -f install/cluster-operator

Finally, you can deploy the cluster custom resource running:

    oc create -f examples/kafka/kafka-ephemeral.yaml

### Local build with push to Minishift Docker registry

When developing locally you might want to push the docker images to the docker repository running in your local OpenShift (minishift) cluster. This can be quicker than pushing to Docker Hub and works even without a network connection.

Assuming your OpenShift login is `developer` (a user with the `cluster-admin` role) and the project is `myproject`, you can push the images to OpenShift's Docker repo using the steps below:

1. Make sure your cluster is running,

        oc cluster up

   By default, you should be logged in as `developer` (you can check this with `oc
   whoami`)

2. Get the *internal* OpenShift docker registry address by running the command below:

        oc get services -n default 
        
   `172.30.1.1:5000` is the typical default IP and port of the Docker registry running in the cluster, however the command above may return a different value.

3. Log in to the Docker repo running in the local cluster:

        docker login -u developer -p $(oc whoami -t) 172.30.1.1:5000
        
   Note that we are using the `developer` OpenShift user and the token for the current (`developer`) login as the password.
        
4. Now run the `docker_push` target to push the development images to that Docker repo. If you need to build/rebuild the Docker images as well, then run the `all` target instead:

        DOCKER_REGISTRY=172.30.1.1:5000 DOCKER_ORG=$(oc project -q) make docker_push
        
5. To use the built images in a deployment, update the `install/cluster-operator/060-Deployment-strimzi-cluster-operator.yml` to obtain the images from the registry at `172.30.1.1:5000`, rather than from DockerHub:

    ```
    sed -Ei -e 's#(image|value): strimzi/([a-z0-9-]+):latest#\1: 172.30.1.1:5000/myproject/\2:latest#' \
            -e 's#([0-9.]+)=strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=172.30.1.1:5000/myproject/\2#' \
            install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml
    ```

    This updates `060-Deployment-strimzi-cluster-operator.yaml`, replacing all the image references (in `image` and `value` properties) with ones with the same name from `172.30.1.1:5000/myproject`.

6. Then you can deploy the Cluster Operator by running:

    oc create -f install/cluster-operator

7. Finally, you can deploy the cluster custom resource by running:

    oc create -f examples/kafka/kafka-ephemeral.yaml

### Local build on Minishift or Minikube

If you do not want to have the docker daemon running on your local development machine, you can build the container images in your minishift or minikube VM by setting your docker host to the address of the VM's daemon:

    eval $(minishift docker-env)

or
    
    eval $(minikube docker-env)

The images will then be built and stored in the cluster VM's local image store and then pushed to your configured Docker registry. 

#### Skipping the registry push

You can avoid the `docker_push` step and `sed` commands above by configuring the Docker Host as above and then running:

    make docker_build

    make DOCKER_ORG=strimzi docker_tag

This labels your latest container build as `strimzi/operator:latest` and you can then deploy the standard CRDs without changing the image targets. However, this will only work if all instances of the `imagePullPolicy:` setting are set to `IfNotPresent` or `Never`. If not, then the cluster nodes will go to the upstream registry (Docker Hub by default) and pull the official images instead of using your freshly built image. 

## Helm Chart

The `strimzi-kafka-operator` Helm Chart can be installed directly from its source.

For any Helm version 2 chart, run:

    `helm install ./helm-charts/helm2/strimzi-kafka-operator`

For any Helm version 3 chart, run:

    `helm install ./helm-charts/helm3/strimzi-kafka-operator`
    
The chart is also available in the release artifact as a tarball.

## Running system tests

System tests has its own guide with more information. See [Testing Guide](TESTING.md) document for more information.

## DCO Signoff

The project requires that all commits are signed-off, indicating that _you_ certify the changes with the developer certificate of origin (DCO) (https://developercertificate.org/). 
This can be done using `git commit -s` for each commit in your pull request. 
Alternatively, to signoff a bunch of commits you can use `git rebase --signoff _your-branch_`.

You can add a commit-msg hook to warn you if the commit you just made locally has not been signed off. Add the following line to you `.git/hooks/commit-msg` script to print the warning:

```
./tools/git-hooks/signoff-warning-commit-msg $1
```

## Checkstyle pre-commit hook

The Checkstyle plugin runs on all pull requests to the Strimzi repository. If you haven't compiled the code via maven, before you submit the PR, then formatting bugs can slip through and this can lead to annoying extra pushes to fix things. In the first instance you should see if your IDE has a Checkstyle plugin that can highlight errors in-line, such as [this one](https://plugins.jetbrains.com/plugin/1065-checkstyle-idea) for IntelliJ.

You can also run the Checkstyle plugin for every commit you make by adding a pre-commit hook to your local Strimzi git repository. To do this, add the following line to your `.git/hooks/pre-commit` script, to execute the checks and fail the commit if errors are detected:

```
./tools/git-hooks/checkstyle-pre-commit
```

## IDE build problems

The build also uses a Java annotation processor. Some IDEs (such as IntelliJ's IDEA) by default don't run the annotation processor in their build process. You can run `mvn clean install -DskipTests -DskipITs` to run the annotation processor as part of the `maven` build and the IDE should then be able to use the generated classes. It is also possible to configure the IDE to run the annotation processor directly.

## Building container images for other platforms with Docker `buildx`

Docker supports building images for different platforms using the `docker buildx` command.
If you want to use it to build Strimzi images, you can just set the environment variable `DOCKER_BUILDX` to `buildx`, set the environment variable `DOCKER_BUILD_ARGS` to pass additional build options such as the platform and run the build.
For example following can be used to build Strimzi images for Linux on Arm64 / AArch64:

```
export DOCKER_BUILDX=buildx
export DOCKER_BUILD_ARGS="--platform linux/amd64 --load"
make all
```

_Note: Strimzi currently does not officially support any other platforms then Linux on `amd64`._
