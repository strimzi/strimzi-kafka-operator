# Developing Strimzi

This document gives a detailed breakdown of the various build processes and options for building Strimzi from source. For a quick start guide see the [Getting Started](DEV_QUICK_START.md) document.

<!-- TOC depthFrom:2 -->

- [Build Pre-requisites](#build-pre-requisites)
- [Make targets](#make-targets)
- [Docker build options](#docker-build-options)
- [Building Strimzi](#building-strimzi)
- [Helm Chart](#helm-chart)
- [Running system tests](#running-system-tests)
- [DCO Signoff](#cdo-signoff)
- [IDE build problems](#ide-build-problems)

<!-- /TOC -->

## Build Pre-Requisites

To build this project you must first install several command line utilities and a Kubernetes or OpenShift cluster.

You may want to try to use a ready-made Docker image based on CentOS 7 with all the tools pre-installed to quickly perform a build. See the instructions [here](HACKING-cli-image.md).

#### Command line tools

- [`make`](https://www.gnu.org/software/make/) - Make build system
- [`mvn`](https://maven.apache.org/index.html) (version 3.5 and above) - Maven CLI
- [`helm`](https://helm.sh/) - Helm Package Management System for Kubernetes
    - After installing Helm be sure to run `helm init`.
- [`asciidoctor`](https://asciidoctor.org/) - Documentation generation. 
    - Use `gem` to install latest version for your platform.
- [`yq`](https://github.com/mikefarah/yq) - YAML manipulation tool. 
    - **Warning:** There are several different `yq` YAML projects in the wild. Use [this one](https://github.com/mikefarah/yq). You need **v3** version.
- [`docker`](https://docs.docker.com/install/) - Docker command line client

In order to use `make` these all need to be available in your `$PATH`.

##### Mac OS

The `make` build is using GNU versions of `find` and `sed` utilities and is not compatible with the BSD versions available on Mac OS. When using Mac OS, you have to install the GNU versions of `find` and `sed`. When using `brew`, you can do `brew install gnu-sed findutils grep coreutils`. This command will install the GNU versions as `gcp`, `ggrep`, `gsed` and `gfind` and our `make` build will automatically pick them up and use them.

The build requires `bash` version 4+ which is not shipped Mac OS but can be installed via homebrew. You can run `brew install bash` to install a compatible version of `bash`. If you wish to change the default shell to the updated bash run `sudo bash -c 'echo /usr/local/bin/bash >> /etc/shells'` and `chsh -s /usr/local/bin/bash`

#### Kubernetes or OpenShift Cluster

In order to run the integration tests and test any changes made to the operators you will need a functioning Kubernetes or OpenShift cluster. This can be a remote cluster or a local development cluster.

##### Minishift 

In order to perform the operations necessary for the integration tests, your user must have the cluster administrator role assigned. For example, if your username is `developer`, you can add the `cluster-admin` role using the commands below:

    oc login -u system:admin

    oc adm policy add-cluster-role-to-user cluster-admin developer
    
    oc login -u developer -p <password>

##### Minikube

The default minishift setup should allow the integration tests to be run without additional configuration changes. 

Sometimes however, updates to minikube may prevent the test cluster context being correctly set. If you have errors due to `oc` commands being called on your minikube cluster then explicitly set the `TEST_CLUSTER` environment variable before running the `make` commands.

    export TEST_CLUSTER=minikube

##### Kubernetes Kind

Currently the integration tests don't run with Kubernetes Kind, so you have to build by using:

    MVN_ARGS=-DskipTests make clean docker_build

## Make targets

Strimzi includes a `Makefile` with various Make targets to build the project.

Commonly used Make targets:

 - `docker_build` for [building Docker images](#building-docker-images)
 - `docker_tag` for [tagging existing images](#tagging-and-pushing-docker-images)
 - `docker_push` for [pushing images to a Docker registry](#tagging-and-pushing-docker-images)

>*Note*: If you are having trouble running any of the Make commands it may help to run `mvn clean` and then `mvn install -DskipTests -DskipITs` before running the commands again.

#### Building Docker images

The `docker_build` target will build the Docker images provided by the Strimzi project. You can build all Strimzi Docker images by calling `make docker_build` from the root of the Strimzi repository. Or you can build an individual Docker image by running `make docker_build` from the subdirectories with their respective Dockerfiles - e.g. `kafka_base`, `kafka` etc.

The `docker_build` target will **always** build the images under the `strimzi` organization. This is necessary in order to be able to reuse the base image you might have just built without modifying all Dockerfiles. The `DOCKER_TAG` environment variable configures the Docker tag to use (default is `latest`).

#### Tagging and pushing Docker images

Target `docker_tag` tags the Docker images built by the `docker_build` target. This target is automatically called as part of the `docker_push` target, but can be called separately if you wish to avoid pushing images to an external registry.

To configure the `docker_tag` and `docker_push` targets you can set following environment variables:
* `DOCKER_ORG` configures the Docker organization for tagging/pushing the images (defaults to the value of the `$USER` environment variable)
* `DOCKER_TAG` configured Docker tag (default is `latest`)
* `DOCKER_REGISTRY` configures the Docker registry where the image will be pushed (default is `docker.io`)

## Docker build options

When building the Docker images you can use an alternative JRE or use an alternate base image.

#### Alternative Docker image JRE

The docker images can be built with an alternative java version by setting the environment variable `JAVA_VERSION`.  For example, to build docker images that have the java 11 JRE installed use `JAVA_VERSION=11 make docker_build`.  If not present, JAVA_VERSION is defaulted to **1.8.0**.

If `JAVA_VERSION` environment variable is set, a profile in the parent pom.xml will set the `maven.compiler.source` and `maven.compiler.target` properties.

#### Alternative `docker` command

The build assumes the `docker` command is available on your `$PATH`. You can set the `DOCKER_CMD` environment variable to use a different `docker` binary or an alternative implementation such as [`podman`](https://podman.io/).

#### Alternative Docker base image

The docker images can be built with an alternative container OS version by adding the environment variable `ALTERNATE_BASE`.  When this environment variable is set, for each component the build will look for a Dockerfile in the subdirectory named by `ALTERNATE_BASE`.  For example, to build docker images based on alpine, use `ALTERNATE_BASE=alpine make docker_build`.  Alternative docker images are an experimental feature not supported by the core Strimzi team.


## Building Strimzi

In order to build Strimzi images you need a running Docker daemon (either local or remote).

If you also want to run tests or deploy Strimzi CRDs and the Strimzi Cluster Operator you need access to a Kubernetes API server.

The `make all` target can be used to trigger both the `docker_build` and the `docker_push` targets described above. This will build the Docker images, tag them and push them to the configured repository.

The build can be customised by:

 - building fewer [Kafka versions](#kafka-versions)
 - customising the [Maven settings](#maven-settings)

Other build options:

 - [Local build with push to Docker Hub](#local-build-with-push-to-docker-hub)
 - [Local build with push to Minishift Docker registry](#local-build-with-push-to-minishift-docker-registry)
 - [Local build on Minishift or Minikube](#local-build-on-minishift-or-minikube)
 - [Local build pushing to the Docker registry used by Kind](#local-build-pushing-to-the-docker-registry-used-by-kind)

#### Kafka versions

As part of the Docker image build several different versions of Kafka will be built, which can increase build times. Which Kafka versions are to be built are defined in the [kafka-versions.yaml](https://github.com/strimzi/strimzi-kafka-operator/blob/master/kafka-versions.yaml) file. Unwanted versions can be commented out to speed up the build process. 

#### Maven Settings

Running `make` invokes Maven for packaging Java based applications (that is, Cluster Operator, Topic Operator, etc). The `mvn` command can be customized by setting the `MVN_ARGS` environment variable when launching `make all`. For example, `MVN_ARGS=-DskipTests make all` can be used to avoid running the unit tests and adding `-DskipIT` will skip the integration tests.

#### Local build with push to Docker Hub

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

#### Local build with push to Minishift Docker registry

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
   
   If everything went right, there should be the built images in your local Docker Registry.

        docker images | grep 172.30.1.1:5000
   
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

#### Local build on Minishift or Minikube

If you do not want to have the docker daemon running on your local development machine, you can build the container images in your minishift or minikube VM by setting your docker host to the address of the VM's daemon:

    eval $(minishift docker-env)

or
    
    eval $(minikube docker-env)

The images will then be built and stored in the cluster VM's local image store and then pushed to your configured Docker registry. 

#### Local build pushing to the Docker registry used by Kind

When developing locally you might want to push the docker images to the docker repository running as a container in your Docker daemon and used by your Kind cluster deployed in the same Docker daemon. This can be quicker and more convenient than pushing to Docker Hub and works even without a network connection.

1. Determine the IP where your Docker Registry can be reached from both your local environment and from docker containers.

   When deploying Docker Registry container to Docker it will most likely be insecure (accessed over 'http://' url).
   There are networking differences with different Docker deployments on different local environments.

   On Linux:

       export REGISTRY_IP=$(ifconfig docker0 | grep 'inet ' | awk '{print $2}') && echo $REGISTRY_IP

   On MacOS:

       export REGISTRY_IP=$(ifconfig en0 | grep 'inet ' | awk '{print $2}') && echo $REGISTRY_IP

2. Deploy the Docker Registry if it has not been deployed yet

       export REGISTRY_NAME=docker-registry
       export REGISTRY_PORT=5000

       docker run -d --restart=always -p "$REGISTRY_PORT:$REGISTRY_PORT" --name "$REGISTRY_NAME" registry:2

   Using `-p` exposes the registry on port 5000 on localhost, but also on the network interface with $REGISTRY_IP address, making it available from your local host as well as from the Docker daemon running in a VM, and also from Docker containers running inside the Docker daemon (such as the Kind cluster running as the `kind-control-plane` container).

3. Configure your Docker daemon to trust your Docker Registry.

   The way we deployed the Docker Registry here is without configuring `https`, keystores, and certificates. Such a registry is considered `insecure`, and requires additional configuration of Docker daemon, and Kind cluster.
   
   On Linux there is usually a daemon config file at `/etc/docker/daemon.json`.

   You should add the `insecure-registries` section to it. For example:

   ```
   {
     "debug": true,
     "experimental": false,
     "insecure-registries": [
       "REGISTRY_IP:5000"          <<< Replace REGISTRY_IP with actual IP from step 1
     ]
   }
   ```

   On MacOS if you're using Docker Desktop you can find this under 'Preferences' / 'Docker Engine'

   After changing this configuration you have to restart the Docker daemon.

4. Make sure you can push locally to your Docker Registry

   Execute the following, to make sure your Docker daemon can work with your Docker Registry

   ```
   docker pull gcr.io/google-samples/hello-app:1.0
   docker tag gcr.io/google-samples/hello-app:1.0 $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
   docker push $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
   ```

5. Start your Kind cluster to work with your Docker Registry

   Rather than simply starting your Kind cluster with `kind create cluster` use the following:

   ```
   export KIND_CLUSTER_NAME=kind

   cat << EOF | kind create cluster --name "${KIND_CLUSTER_NAME}" --config=-
   kind: Cluster
   apiVersion: kind.x-k8s.io/v1alpha4
   containerdConfigPatches: 
   - |-
     [plugins."io.containerd.grpc.v1.cri".registry.mirrors."$REGISTRY_IP:$REGISTRY_PORT"]
       endpoint = ["http://$REGISTRY_IP:$REGISTRY_PORT"]
   EOF
   ```

   Note, how we use `http` in the `endpoint` value, which makes Kind work with your 'insecure' registry.

6. Make sure Kind can deploy images from your Docker Registry

   If the following works and results in a successful deployment of `hello-server` (status RUNNING) then you're all set.

   ```
   docker tag gcr.io/google-samples/hello-app:1.0 $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
   kubectl create deployment hello-server --image=$REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
   kubectl get pod
   ```
   
   You may need to repeat the last command a few times to make sure the pod reaches `Running` status.

   Then, remove the deployment:

       kubectl delete deployment hello-server

7. Now run the `docker_push` target to push the development images to that Docker repo. If you need to build/rebuild the Docker images as well, then run the `all` target instead:

       export DOCKER_REG=$REGISTRY_IP:$REGISTRY_PORT
       DOCKER_REGISTRY=$DOCKER_REG DOCKER_ORG=strimzi make docker_push
        
8. In order to use the built images in a deployment, you need to update the `install/cluster-operator/050-Deployment-strimzi-cluster-operator.yml` to obtain the images from the registry at `$REGISTRY_IP:$REGISTRY_PORT`, rather than from DockerHub.

   That can be done using the following command:

   ```
   sed -Ei -e "s#(image|value): strimzi/([a-z0-9-]+):latest#\1: ${DOCKER_REG}/strimzi/\2:latest#" \
       -e "s#([0-9.]+)=strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=${DOCKER_REG}/strimzi/\2#" \
       install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml
   ```

   This will update `050-Deployment-strimzi-cluster-operator.yaml` replacing all the image references (in `image` and `value` properties) with ones with the same name from `$REGISTRY_IP:$REGISTRY_PORT/strimzi`.

9. Change the `RoleBindings` and `ClusterRoleBindinga` namespace to `default`

        sed -Ei -e 's/namespace: .*/namespace: default/' install/cluster-operator/*.yaml

   As an alternative you may want to create your own namespace, and use that.

   For example:
   
        kubectl create ns kafka
        sed -Ei -e 's/namespace: .*/namespace: kafka/' install/cluster-operator/*.yaml

   But, you then have to use `-n kafka` when issuing `kubectl` commands.
   
10. Then you can deploy the Cluster Operator by running:

        kubectl create -f install/cluster-operator

    You can follow its status by executing:
        
        kubectl get events -w | grep operator
        
11. Finally, you can deploy the cluster custom resource by running:

        kubectl create -f examples/kafka/kafka-ephemeral.yaml

##### Skipping the registry push

You can avoid the `docker_push` step and `sed` commands above by configuring the Docker Host as above and then running:

    make docker_build

    make DOCKER_ORG=strimzi docker_tag

This labels your latest container build as `strimzi/operator:latest` and you can then deploy the standard CRDs without changing the image targets. However, this will only work if all instances of the `imagePullPolicy:` setting are set to `IfNotPresent` or `Never`. If not, then the cluster nodes will go to the upstream registry (Docker Hub by default) and pull the official images instead of using your freshly built image. 

## Helm Chart

The `strimzi-kafka-operator` Helm Chart can be installed directly from its source.

    helm install ./helm-charts/strimzi-kafka-operator
    
The chart is also available in the release artifact as a tarball.

## Running system tests

System tests has its own guide with more information. See [Testing Guide](TESTING.md) document for more information.

## DCO Signoff

The project requires that all commits are signed-off, indicating that _you_ certify the changes with the developer certificate of origin (DCO) (https://developercertificate.org/). 
This can be done using `git commit -s` for each commit in your pull request. 
Alternatively, to signoff a bunch of commits you can use `git rebase --signoff _your-branch_`.

## IDE build problems

The build also uses a Java annotation processor. Some IDEs (such as IntelliJ) don't, by default, run the annotation processor in their build process. You can run `mvn clean install -DskipTests -DskipITs` to run the annotation processor as part of the `maven` build and the IDE should then be able to use the generated classes. It is also possible to configure the IDE to run the annotation processor directly.
