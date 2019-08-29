# Developing Strimzi

This document gives a detailed breakdown of the various build processes and options for building Strimzi from source. For a quick start guide see the [Getting Started](https://github.com/strimzi/strimzi-kafka-operator/blob/master/DEV_QUICK_START.md) document.

<!-- TOC depthFrom:2 -->

- [Build Pre-requisites](#build-pre-requisites)
- [Make targets](#make-targets)
- [Docker options](#docker-options)
- [Building Strimzi](#building-strimzi)
- [Helm Chart](#helm-chart)
- [Running system tests](#running-system-tests)

<!-- /TOC -->

## Build Pre-Requisites

To build this project you must first install several command line utilities.

- [`make`](https://www.gnu.org/software/make/) - Make build system
- [`mvn`](https://maven.apache.org/index.html) (version 3.5 and above) - Maven CLI
- [`helm`](https://helm.sh/) - Helm Package Management System for Kubernetes
    - After installing Helm be sure to run `helm init`.
- [`asciidoctor`](https://asciidoctor.org/) - Documentation generation. 
    - Use `gem` to install latest version for your platform.
- [`yq`](https://github.com/mikefarah/yq) - YAML manipulation tool. 
    - **Warning:** There are several different `yq` YAML projects in the wild. Use [this one](https://github.com/mikefarah/yq).
- [`docker`](https://docs.docker.com/install/) - Docker command line client

In order to use `make` these all need to be available in your `$PATH`.

### Mac OS

The `make` build is using GNU versions of `find` and `sed` utilities and is not compatible with the BSD versions available on Mac OS. When using Mac OS, you have to install the GNU versions of `find` and `sed`. When using `brew`, you can do `brew install gnu-sed findutils grep coreutils`. This command will install the GNU versions as `gcp`, `ggrep`, `gsed` and `gfind` and our `make` build will automatically pick them up and use them.

The build requires `bash` version 4+ which is not shipped Mac OS but can be installed via homebrew. You can run `brew install bash` to install a compatible version of `bash`. If you wish to change the default shell to the updated bash run `sudo bash -c 'echo /usr/local/bin/bash >> /etc/shells'` and `chsh -s /usr/local/bin/bash`  

### IDE

The build also uses a Java annotation processor. Some IDEs (such as IntelliJ) don't, by default, run the annotation processor in their build process. You can run `mvn clean install -DskipTests -DskipITs` to run the annotation processor as part of the `maven` build and the IDE should then be able to use the generated classes. It is also possible to configure the IDE to run the annotation processor directly.

### Cluster

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

### Building Docker images

The `docker_build` target will build the Docker images provided by the Strimzi project. You can build all Strimzi Docker images by calling `make docker_build` from the root of the Strimzi repository. Or you can build an individual Docker image by running `make docker_build` from the subdirectories with their respective Dockerfiles - e.g. `kafka_base`, `kafka` etc.

The `docker_build` target will **always** build the images under the `strimzi` organization. This is necessary in order to be able to reuse the base image you might have just built without modifying all Dockerfiles. The `DOCKER_TAG` environment variable configures the Docker tag to use (default is `latest`).

#### Kafka versions

As part of the Docker image build several different versions of Kafka will be built, which can increase build times. Which Kafka versions are to be built are defined in the [kafka-versions](https://github.com/strimzi/strimzi-kafka-operator/blob/master/kafka-versions) file. Unwanted versions can be commented out to speed up the build process.

### Tagging and pushing Docker images

Target `docker_tag` can be used to tag the Docker images built by the `docker_build` target. This target is automatically called by the `docker_push` target and doesn't have to be called separately. 

To configure the `docker_tag` and `docker_push` targets you can set following environment variables:
* `DOCKER_ORG` configures the Docker organization for tagging/pushing the images (defaults to the value of the `$USER` environment variable)
* `DOCKER_TAG` configured Docker tag (default is `latest`)
* `DOCKER_REGISTRY` configures the Docker registry where the image will be pushed (default is `docker.io`)

## Docker options

### Alternate Docker image JRE

The docker images can be built with an alternate java version by setting the environment variable `JAVA_VERSION`.  For example, to build docker images that have the java 11 JRE installed use `JAVA_VERSION=11 make docker_build`.  If not present, JAVA_VERSION is defaulted to **1.8.0**.

If `JAVA_VERSION` environment variable is set, a profile in the parent pom.xml will set the `maven.compiler.source` and `maven.compiler.target` properties.

### Alternate Docker base image

The docker images can be built with an alternate container OS version by adding the environment variable `ALTERNATE_BASE`.  When this environment variable is set, for each component the build will look for a Dockerfile in the subdirectory named by `ALTERNATE_BASE`.  For example, to build docker images based on alpine, use `ALTERNATE_BASE=alpine make docker_build`.  Alternate docker images are an experimental feature not supported by the core Strimzi team.

## Building Strimzi

The `make all` target can be used to trigger both the `docker_build` and the `docker_push` targets described above. This will build the Docker images, tag them and push them to the configured repository. 

### Maven 

Running `make` invokes Maven for packaging Java based applications (that is, Cluster Operator, Topic Operator, etc). The `mvn` command can be customized by setting the `MVN_ARGS` environment variable when launching `make all`. For example, `MVN_ARGS=-DskipTests make all` can be used to avoid running the unit tests and adding `-DskipIT` will skip the integration tests.

### Local build with push to Docker Hub

To build the images on your local machine and push them to the Docker Hub, log into Docker Hub with your account details. This sets the Hub as the `docker push` registry target.

    docker login

By default the `docker_push` target will build the images under the strimzi organisation (e.g. `strimzi/operator:latest`) and attempt to push them to the strimzi repositories on the Docker Hub. Only certain users are approved to do this so you should push to your own Docker Hub organisation (account) instead. To do this, make sure that the `DOCKER_ORG` environment variable is set to the same value as your username on Docker Hub before running the `make` commands.

    export DOCKER_ORG=docker_hub_username

When the Docker images are build then will now be labeled in the form: `docker_hub_username/operator:latest` in your local repository and pushed to your Docker Hub account under the same label.

In order to use these newly built images, you need to update the `install/cluster-operator/050-Deployment-strimzi-cluster-operator.yml` to obtain the images from your repositories on Docker Hub rather than the official Strimzi images. That can be done using the following command and replacing `docker_hub_username` with the relevant value:

```
sed -Ei -e 's#(image|value): strimzi/([a-z0-9-]+):latest#\1: docker_hub_username/\2:latest#' \
        -e 's#([0-9.]+)=strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=docker_hub_username/\2#' \
        install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml
```

This will update `050-Deployment-strimzi-cluster-operator.yaml` replacing all the image references (in `image` and `value` properties) with ones with the same name but with the repository changed.

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
        
   `172.30.1.1:5000` is the typical default IP and port of the Docker registry running in the cluster, however the command may above may return a different value.

3. Log in to the Docker repo running in the local cluster:

        docker login -u developer -p $(oc whoami -t) 172.30.1.1:5000
        
   Note that we are using the `developer` OpenShift user and the token for the current  (`developer`) login as the password. 
        
4. Now run the `docker_push` target to push the development images to that Docker repo. If you need to build/rebuild the Docker images as well, then run the `all` target instead:

        DOCKER_REGISTRY=172.30.1.1:5000 DOCKER_ORG=$(oc project -q) make docker_push
        
5. In order to use the built images in a deployment, you need to update the `install/cluster-operator/050-Deployment-strimzi-cluster-operator.yml` to obtain the images from the registry at `172.30.1.1:5000`, rather than from DockerHub. That can be done using the following command:

    ```
    sed -Ei -e 's#(image|value): strimzi/([a-z0-9-]+):latest#\1: 172.30.1.1:5000/myproject/\2:latest#' \
            -e 's#([0-9.]+)=strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=172.30.1.1:5000/myproject/\2#' \
            install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml
    ```

    This will update `050-Deployment-strimzi-cluster-operator.yaml` replacing all the image references (in `image` and `value` properties) with ones with the same name from `172.30.1.1:5000/myproject`.

6. Then you can deploy the Cluster Operator by running:

    oc create -f install/cluster-operator

7. Finally, you can deploy the cluster custom resource by running:

    oc create -f examples/kafka/kafka-ephemeral.yaml

### Cluster build

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

    helm install ./helm-charts/strimzi-kafka-operator
    
The chart is also available in the release artifact as a tarball.

## Running system tests

### Test groups

To execute an expected group of system tests need to add system property `junitTags` with following value:

`-DjunitTags=integration` - to execute one test group
`-DjunitTags=acceptance,regression` - to execute many test groups
`-DjunitTags=all` - to execute all test groups

If `junitTags` system property isn't defined, all tests without an explicitly declared test group will be executed. The following table shows currently used tags:

| Name          | Description                                                                        |
| :-----------: | :--------------------------------------------------------------------------------: |
| travis        | Marks tests executed on Travis                                                     |
| acceptance    | Acceptance tests, which guarantee, that basic functionality of Strimzi is working. |
| regression    | Regression tests, which contains all non-flaky tests.                              |
| upgrade       | Upgrade tests for specific versions of the Strimzi.                                |
| flaky         | Execute all flaky tests (tests, which are failing from time to time)               |
| all           | Execute all system tests with any tag                                              |

### Helper script

The `./systemtest/scripts/run_tests.sh` script can be used to run the `systemtests` using the same configuration as used in the travis build.  You can use this script to easily run the `systemtests` project.

Pass additional parameters to `mvn` by populating the `EXTRA_ARGS` env var.

    EXTRA_ARGS="-Dfoo=bar" ./systemtest/scripts/run_tests.sh
    
### Running single test class

Use the `test` build goal and provide a `-Dtest=TestClassName[#testMethodName]` system property. 

    mvn verify -pl systemtest -P systemtests -Djava.net.preferIPv4Stack=true -DtrimStackTrace=false -DjunitTags=acceptance,regression -Dtest=KafkaST#testKafkaAndZookeeperScaleUpScaleDown


### Environment variables

We can configure our system tests with several environment variables, which are loaded before test execution:

| Name                      | Description                                                                          | Default                                          |
| :-----------------------: | :----------------------------------------------------------------------------------: | :----------------------------------------------: |
| DOCKER_ORG                | Specify organization which owns image used in system tests                           | strimzi                                          |
| DOCKER_TAG                | Specify image tags used in system tests                                              | latest                                           |
| DOCKER_REGISTRY           | Specify docker registry used in system tests                                         | docker.io                                        |
| TEST_CLIENT_IMAGE         | Specify test client image used in systemtest                                         | docker.io/strimzi/test-client:latest-kafka-2.3.0 |
| BRIDGE_IMAGE              | Specify kafka bridge image used in systemtest                                        | docker.io/strimzi/kafka-bridge:latest            |
| TEST_LOG_DIR              | Directory for store logs collected during the tests                                  | ../systemtest/target/logs/                       |
| ST_KAFKA_VERSION          | Kafka version used in images during the system tests                                 | 2.3.0                                            |
| STRIMZI_DEFAULT_LOG_LEVEL | Log level for cluster operator                                                       | DEBUG                                            |
| KUBERNETES_DOMAIN         | Cluster domain. It's used for specify URL endpoint of testing clients                | .nip.io                                          |

If you want to use your own images with different tag or from different repository, you can use `DOCKER_REGISTRY`, `DOCKER_ORG` and `DOCKER_TAG` environment variables.

`KUBERNETES_DOMAIN` should be specified only in case you are using specific configuration in your kubernetes cluster.

#### Specific Kafka version

To set custom Kafka version in system tests need to add system property `ST_KAFKA_VERSION` with one of the following values: `2.1.0`, `2.1.1`, `2.2.0`, `2.2.1`, `2.3.0`. For more info about allowed versions see [kafka-versions](https://github.com/strimzi/strimzi-kafka-operator/blob/master/kafka-versions).

#### Cluster Operator Log level

To set the log level of Strimzi for system tests need to add system property `STRIMZI_DEFAULT_LOG_LEVEL` with one of the following values: `ERROR`, `WARNING`, `INFO`, `DEBUG`, `TRACE`.

### Test Cluster

The integration and system tests are run against a cluster specified in the environment variable `TEST_CLUSTER_CONTEXT`. If this variable is not set, kubernetes client will use currently active context. Otherwise will use context from kubeconfig with name specified by `TEST_CLUSTER_CONTEXT` variable.

For example command `TEST_CLUSTER_CONTEXT=remote-cluster ./systemtest/scripts/run_tests.sh` will execute tests with cluster context `remote-cluster`. However, since system tests use command line `Executor` for some actions, make sure that you are using context from `TEST_CLUSTER_CONTEXT`.

System tests uses admin user for some actions. You can specify admin user via variable `TEST_CLUSTER_ADMIN` (by default it use `developer` because `system:admin` cannot be used over remote connections).

## DCO Signoff

The project requires that all commits are signed-off, indicating that _you_ certify the changes with the developer certificate of origin (DCO) (https://developercertificate.org/). 
This can be done using `git commit -s` for each commit in your pull request. 
Alternatively, to signoff a bunch of commits you can use `git rebase --signoff _your-branch_`.