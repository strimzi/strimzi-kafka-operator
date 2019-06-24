# Building Strimzi

Strimzi is using `make` as its main build system. Our make build supports 
several different targets mainly for building and pushing Docker images.

The build also uses an Java annotation processor. Some IDEs (such as IntelliJ) doesn't, by default, run the annotation processor in their build process. You can run `mvn clean install -DskipTests -DskipITs` to run the annotation processor as the maven build and the IDE should then be able to use the generated classes. It is also possible to configure the IDE to run the annotation processor itself.

<!-- TOC depthFrom:2 -->

- [Build Pre-requisites](#build-pre-requisites)
- [Docker images](#docker-images)
    - [Building Docker images](#building-docker-images)
    - [Alternate Docker image JRE](#alternate-docker-image-jre)
    - [Tagging and pushing Docker images](#tagging-and-pushing-docker-images)
- [Building everything](#building-everything)
- [Pushing images to the cluster's Docker repo](#pushing-images-to-the-clusters-docker-repo)
- [Helm Chart](#helm-chart)
- [Release](#release)
- [Running system tests](#running-system-tests)

<!-- /TOC -->

## Build Pre-Requisites

To build this project you must first install several command line utilities.

- [`make`](https://www.gnu.org/software/make/) - Make build system
- [`mvn`](https://maven.apache.org/index.html) - Maven CLI
- [`helm`](https://helm.sh/) - Helm Package Management System for Kubernetes
    - After installing Helm be sure to run `helm init`.
- [`asciidoctor`](https://asciidoctor.org/) - Documentation generation (use `gem` to install latest version for your platform)
- [`yq`](https://github.com/mikefarah/yq) - YAML manipulation tool.  **Warning: There are several different `yq` yaml projects in the wild.  [Use this one.](https://github.com/mikefarah/yq)** 

In order to use `make` these all need to be available in your `$PATH`.

### Mac OS

The `make` build is using GNU versions of `find` and `sed` utilities and is not compatible with the BSD versions available on Mac OS. 
When using Mac OS, you have to install the GNU versions of `find` and `sed`.
When using `brew`, you can do `brew install gnu-sed findutils grep coreutils`.
This command will install the GNU versions as `gcp`, `ggrep`, `gsed` and `gfind` and our `make` build will automatically pick them up and use them.

The build requires `bash` version 4+ which is not shipped Mac OS but can be installed via homebrew. 
You can run `brew install bash` to install a compatible version of `bash`.
If you wish to change the default shell to the updated bash run `sudo bash -c 'echo /usr/local/bin/bash >> /etc/shells'` and `chsh -s /usr/local/bin/bash`  
                                                                                            

## Docker images

### Building Docker images

The `docker_build` target will build the Docker images provided by the 
Strimzi project. You can build all Strimzi Docker images by calling 
`make docker_build` from the root of the Strimzi repository. Or you can build 
an individual Docker image by running `make docker_build` from the 
subdirectories with their respective Dockerfiles - e.g. `kafka_base`, 
`kafka` etc.

The `docker_build` target will always build the images under the 
`strimzi` organization. This is necessary in order to be able to reuse 
the base image you might have just built without modifying all Dockerfiles. 
The `DOCKER_TAG` environment variable configures the Docker tag 
to use (default is `latest`).

### Alternate Docker image JRE

The docker images can be built with an alternate java version by setting the environment variable
`JAVA_VERSION`.  For example, to build docker images that have the java 11 JRE installed use
`JAVA_VERSION=11 make docker_build`.  If not present, JAVA_VERSION is defaulted to **1.8.0**.

If `JAVA_VERSION` environment variable is set, a profile in the parent pom.xml will set the
`maven.compiler.source` and `maven.compiler.target` properties.

### Alternate Docker base image

The docker images can be built with an alternate container OS version by adding the environment
variable ```ALTERNATE_BASE```.  When this environment variable is set, for each component the build
will look for a Dockerfile in the subdirectory named by ```ALTERNATE_BASE```.  For example, to build
docker images based on alpine, use `ALTERNATE_BASE=alpine make docker_build`.  Alternate docker
images are an experimental feature not supported by the core Strimzi team.

### Tagging and pushing Docker images

Target `docker_tag` can be used to tag the Docker images built by the 
`docker_build` target. This target is automatically called by the `docker_push` 
target and doesn't have to be called separately. 

To configure the `docker_tag` and `docker_push` targets you can set following 
environment variables:
* `DOCKER_ORG` configures the Docker organization for tagging/pushing the 
  images (defaults to the value of the `$USER` environment variable)
* `DOCKER_TAG` configured Docker tag (default is `latest`)
* `DOCKER_REGISTRY` configures the Docker registry where the image will 
  be pushed (default is `docker.io`)

## Building everything

`make all` command can be used to trigger all the tasks above - build the 
Docker images, tag them and push them to the configured repository.

`make` invokes Maven for packaging Java based applications (that is, Cluster Operator, Topic Operator, ...). 
The `mvn` command can be customized by setting the `MVN_ARGS` environment variable when launching `make all`. 
For example, `MVN_ARGS=-DskipTests make all` can be used to avoid running the unit tests.

## Pushing images to the cluster's Docker repo

When developing locally you might want to push the docker images to the docker
repository running in your local OpenShift cluster. This can be quicker than
pushing to dockerhub and works even without a network connection.

Assuming your OpenShift login is `developer` and project is `myproject` 
you can push the images to OpenShift's Docker repo like this:

1. Make sure your cluster is running,

        oc cluster up

   By default, you should be logged in as `developer` (you can check this 
   with `oc whoami`)
        
2. Log in to the Docker repo running in the local cluster:

        docker login -u developer -p `oc whoami -t` 172.30.1.1:5000
        
   `172.30.1.1:5000` happens to be the IP and port of the Docker registry
   running in the cluster (see `oc get svc -n default`). Note that we are using the `developer` OpenShift user and the 
   token for the current (`developer`) login as the password. 
        
3. Now run `make` to push the development images to that Docker repo:

        DOCKER_REGISTRY=172.30.1.1:5000 DOCKER_ORG=`oc project -q` make all
        
4. In order to use the built images, you need to update the `install/cluster-operator/050-Deployment-strimzi-cluster-operator.yml` to obtain the images from the registry at `172.30.1.1:5000`, rather than from DockerHub.
  That can be done using the following command:

    ```
    sed -Ei -e 's#(image|value): strimzi/([a-z0-9-]+):latest#\1: 172.30.1.1:5000/myproject/\2:latest#' \
            -e 's#([0-9.]+)=strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=172.30.1.1:5000/myproject/\2#' \
            install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml
    ```

    This will update `050-Deployment-strimzi-cluster-operator.yaml` replacing all the image references (in `image` and `value` properties) with ones with the same name from `172.30.1.1:5000/myproject`.

5. Then you can deploy the Cluster Operator running:

    oc create -f install/cluster-operator

6. Finally, you can deploy the cluster custom resource running:

    oc create -f examples/kafka/kafka-ephemeral.yaml

## Helm Chart

The `strimzi-kafka-operator` Helm Chart can be installed directly from its source.

    helm install ./helm-charts/strimzi-kafka-operator
    
The chart is also available in the release artifact as a tarball.

## Release

`make release` target can be used to create a release. Environment variable 
`RELEASE_VERSION` (default value `latest`) can be used to define the release 
version. The `release` target will:
* Update all tags of Docker images to `RELEASE_VERSION`
* Update documentation version to `RELEASE_VERSION`
* Set version of the main Maven projects (`topic-operator` and `cluster-operator`) to `RELEASE_VERSION` 
* Create TAR.GZ and ZIP archives with the Kubernetes and OpenShift YAML files which can be used for deployment
and documentation in HTML format.
 
The `release` target will not build the Docker images - they should be built and pushed automatically by Travis CI 
when the release is tagged in the GitHub repository. It also doesn't deploy the Java artifacts anywhere. They are only 
used to create the Docker images.

The release process should normally look like this:
1. Create a release branch
2. Export the desired version into the environment variable `RELEASE_VERSION`
3. Run `make clean release`
4. Commit the changes to the existing files (do not add the newly created top level TAR.GZ, ZIP archives or .yaml files into Git)
5. Push the changes to the release branch on GitHub
6. Create the tag and push it to GitHub. Tag name determines the tag of the resulting Docker images. Therefore the Git 
tag name has to be the same as the `RELEASE_VERSION`, i.e. `git tag ${RELEASE_VERSION}`,
7. Once the CI build for the tag is finished and the Docker images are pushed to Docker Hub, Create a GitHub release and tag based on the release branch. 
Attach the TAR.GZ/ZIP archives, YAML files (for installation from URL) from step 4 and the Helm Chart to the release
8. On the `master` git branch
  * Update the versions to the next SNAPSHOT version using the `next_version` `make` target. 
  For example to update the next version to `0.6.0-SNAPSHOT` run: `make NEXT_VERSION=0.6.0-SNAPSHOT next_version`.
  * Copy the `helm-charts/index.yaml` from the `release` branch to `master`.
9. Update the website
  * Add release documentation to `strimzi.github.io/docs/`.
  Update references to docs in `strimzi.github.io/documentation/index.md` and `strimzi.github.io/documentation/archive/index.md`.
  Update also the link from the start page: `strimzi.github.io/index.md`.
  * Update the Helm Chart repository file by copying `strimzi-kafka-operator/helm-charts/index.yaml` to
  `strimzi.github.io/charts/index.yaml`.
  * Update the Quickstarts for OKD and Minikube to use the latest stuff.
10. The maven artifacts (`api` module) will be automatically staged from TravisCI during the tag build. 
It has to be releases from [Sonatype](https://oss.sonatype.org/#stagingRepositories) to get to the main Maven repositories.

## Running system tests

### Test groups

To execute an expected group of system tests need to add system property `junitTags` with following value:

`-DjunitTags=integration` - to execute one test group
`-DjunitTags=acceptance,regression` - to execute many test groups
`-DjunitTags=all` - to execute all test groups

If `junitTags` system property isn't defined, all tests without an explicitly declared test group will be executed. The following table shows currently used tags:

| Name | Description |
| :---: | :---: |
| travis | Marks tests executed on Travis |
| acceptance | Acceptance tests, which guarantee, that basic functionality of Strimzi is working. |
| regression | Regression tests |
| upgrade | Execute upgrade tests |
| pr | Execute subset of tests for pull requests |
| flaky | Test which are flaky |
| cci_flaky | Test which are flaky only on specific QE environment |
| systemtests | Execute all system tests with any tag |

### Helper script

The `./systemtest/scripts/run_tests.sh` script can be used to run the `systemtests` using the same configuration as used 
in the travis build.  You can use this script to easily run the `systemtests` project.

Pass additional parameters to `mvn` by populating the `EXTRA_ARGS` env var.

    EXTRA_ARGS="-Dfoo=bar" ./systemtest/scripts/run_tests.sh
    
### Running single test class

Use the `test` build goal and provide a `-Dtest=TestClassName[#testMethodName]` system property. 

Ex)

    mvn verify -pl systemtest -P systemtests -Djava.net.preferIPv4Stack=true -DtrimStackTrace=false -DjunitTags=acceptance,regression -Dtest=KafkaST#testKafkaAndZookeeperScaleUpScaleDown


### Environment variables

We can configure our system tests with several environment variables, which are loaded before test execution:

| Name | Description | Default |
| :---: | :---: | :---: |
| DOCKER_ORG | Specify organization which owns image used in system tests | strimzi |
| DOCKER_TAG | Specify image tags used in system tests | latest |
| DOCKER_REGISTRY | Specify docker registry used in system tests | docker.io |
| TEST_LOG_DIR | Directory for store logs collected during the tests | ../systemtest/target/logs/ |
| ST_KAFKA_VERSION | Kafka version used in images during the system tests | 2.1.1 |
| STRIMZI_DEFAULT_LOG_LEVEL | Log level for cluster operator | DEBUG |
| KUBERNETES_DOMAIN | Cluster domain. It's used for specify URL endpoint of testing clients | .nip.io |
| KUBERNETES_API_URL | URL of the kubernetes cluster. It's used for specify URL endpoint of testing clients | https://127.0.0.1:8443 |

If you want to use your own images with different tag or from different repository, you can use `DOCKER_REGISTRY`, `DOCKER_ORG` and `DOCKER_TAG` environment variables.

`KUBERNETES_DOMAIN` and `KUBERNETES_API_URL` should be specified only in case you are using specific configuration in your kubernetes cluster.

#### Specific Kafka version

To set custom Kafka version in system tests need to add system property `ST_KAFKA_VERSION` with one of the following values: `2.1.0`, `2.1.1`, `2.2.0`, `2.2.1`. For more info about allowed versions see [kafka-versions](https://github.com/strimzi/strimzi-kafka-operator/blob/master/kafka-versions).

#### Cluster Operator Log level

To set the log level of Strimzi for system tests need to add system property `STRIMZI_DEFAULT_LOG_LEVEL` with one of the following values: `ERROR`, `WARNING`, `INFO`, `DEBUG`, `TRACE`.

### Test Cluster

The integration and system tests are run against a cluster specified in the environment variable `TEST_CLUSTER_CONTEXT`. 
If this variable is not set, kubernetes client will use currently active context. Otherwise will use context from kubeconfig with name specified by `TEST_CLUSTER_CONTEXT` variable.

For example command `TEST_CLUSTER_CONTEXT=remote-cluster ./systemtest/scripts/run_tests.sh` will execute tests with cluster context `remote-cluster`.
However, since system tests use command line `Executor` for some actions, make sure that you are using context from `TEST_CLUSTER_CONTEXT`.

System tests uses admin user for some actions. You can specify admin user via variable `TEST_CLUSTER_ADMIN` (by default it use `developer` because `system:admin` cannot be used over remote connections).
