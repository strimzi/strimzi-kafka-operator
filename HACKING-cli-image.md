Strimzi Development CLI image
=============================

Strimzi Development CLI image is a CentOS 7 based image that contains Java 8 and all the build dependencies and tools needed to build and deploy Strimzi.

In order to build Strimzi Kafka Operator project you need a running Docker daemon.

If you also want to try them out, deploy the Strimzi Cluster Operator, and run a test Kafka cluster on Kubernetes, you'll need access to a Kubernetes API server.

There are several locally running options for Kubernetes as explained in [Strimzi Quickstarts](https://strimzi.io/quickstarts/).

Look for instructions on installing Docker, Kubernetes in those documents or on web.

For using Strimzi Development CLI image you only need a working Docker daemon, and `docker` client.


Starting the session
--------------------

First, make sure that you have the latest version of the image (best to do this every time you start a new session):

    docker pull quay.io/mstruk/strimzi-dev-cli

Second, you may have to prepare some configurations for accessing Docker daemon, Kubernetes API server, Maven repository ... from within your Docker container session.

For example, if you're using Kind you'll want to export its configuration specifically for use from within Docker containers:

    kind get kubeconfig --internal > ~/.kube/internal-kubeconfig

Finally, run the interactive shell mounting local directories you want to share with your container session as volumes.

For example:
* if using Kind for Kubernetes, you'll want to make the kube configuration file available inside the container
* if using your local Docker daemon over a UNIX domain socket, you'll want to make the socket file available inside the container
* typically you want to mount the local directory that contains your strimzi-kafka-operator project so that you can make changes using your locally running IDE
* you may want to use a local directory for storing downloaded maven artifacts (although I/O operations on mounted volumes might be slower than on container's private filesystem)

```
# set DEV_DIR to point to directory where you have your cloned git repositories
# You'll be able to access this directory from within Strimzi Dev CLI container
export DEV_DIR=$HOME/devel

# Run the container interactively
docker run -ti --name strimzi-dev-cli -v $HOME/.kube:/root/.kube -v /var/run/docker.sock:/var/run/docker.sock -v $DEV_DIR:/root/devel -v $HOME/.m2:/root/.m2:cached quay.io/mstruk/strimzi-dev-cli /bin/sh
```

Note: If you exit the container or it gets shut down, as long as it's not manually deleted you can reattach and continue the interactive session:

    docker start strimzi-dev-cli
    docker attach strimzi-dev-cli

Having started the interactive session you are now in the development environment where you have all the necessary tools including `docker`, `kind`, `kubectl`, `git`, `mvn` and all the rest you need to build the Strimzi project.

If you're using Kind for your local Kubernetes cluster you may want to check that everything works:

```
export KUBECONFIG=~/.kube/internal-kubeconfig
kubectl get ns
docker ps
```

Also, if running a local Docker Registry as another docker container [as explained here](HACKING.md#local-build-pushing-to-the-docker-registry-used-by-kind), you'll want to make sure that you can push to it from your interactive container:

```
# Set REGISTRY_IP to the same value you configured on Docker daemon
export REGISTRY_IP=<enter-the-ip-of-your-en0-or-docker0>

export REGISTRY_PORT=5000

# test docker push to local repository
docker tag gcr.io/google-samples/hello-app:1.0 $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
docker push $REGISTRY_IP:$REGISTRY_PORT/hello-app:1.0
```

Building the project
--------------------

Change to the directory with your sources (you may need to clone the project if you haven't already):

    cd ~/devel/strimzi-kafka-operator

The quickest way to build the project is to run:

    MVN_ARGS="-DskipTests -Dcheckstyle.skip -Dmaven.javadoc.skip" make clean docker_build

You then push the images to your Docker Registry, which depends on your environment. For example:

    export DOCKER_REG=$REGISTRY_IP:$REGISTRY_PORT
    DOCKER_REGISTRY=$DOCKER_REG DOCKER_ORG=strimzi make docker_push


See [HACKING.md](HACKING.md#make-targets) for build targets, and other build instructions.


Deploying the operator and the cluster
--------------------------------------

See instructions in [HACKING.md](HACKING.md#building-strimzi) dependening on your environment.
