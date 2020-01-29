# Developer Quick Start

## Pre-requisites 

To build Strimzi from source you need an Kubernetes or OpenShift cluster available. You can install either [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) or [minishift](https://www.okd.io/minishift/) to have access to a cluster on your local machine.

You will also need access to several command line utilities:

- [`make`](https://www.gnu.org/software/make/) - Make build system
- [`mvn`](https://maven.apache.org/index.html) (version 3.5 and above) - Maven CLI
- [`asciidoctor`](https://asciidoctor.org/) - Documentation generation. 
    - Use `gem` to install latest version for your platform.
- [`yq`](https://github.com/mikefarah/yq) - YAML manipulation tool. 
    - **Warning:** There are several different `yq` YAML projects in the wild. Use
      [this one](https://github.com/mikefarah/yq).
- [`docker`](https://docs.docker.com/install/) - Docker command line client and daemon
- Kubernetes (`kubectl`) or OpenShift (`oc`) command line clients
    - These are provided by both minikube (via `minikube kubectl`) and minishift (with some additional setup) or can be installed separately. 

### Minishift Setup

In order to perform the operations necessary for the integration tests, your user must have the cluster administrator role assigned. For example, if your username is `developer`, you can add the `cluster-admin` role using the commands below:

    oc login -u system:admin

    oc adm policy add-cluster-role-to-user cluster-admin developer
    
    oc login -u developer -p <password>

## Build from source

To build Strimzi from source the operator and Kafka code needs to be compiled into Docker container images and placed in a location accessible to the Kubernetes/OpenShift nodes. The easiest way to make your personal Strimzi builds accessible, is to place them on the [Docker Hub](https://hub.docker.com/). The instructions below use this method, however other build options (including options for limited or no network access) are available in the [HACKING](https://github.com/strimzi/strimzi-kafka-operator/blob/master/HACKING.md) document. The commands below should work for both minishift and minikube clusters:

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

4. In order to use the newly built images, you need to update the `install/cluster-operator/050-Deployment-strimzi-cluster-operator.yml` to obtain the images from your repositories on Docker Hub rather than the official Strimzi images. That can be done using the following command:

    ```
    sed -Ei -e "s#(image|value): strimzi/([a-z0-9-]+):latest#\1: $DOCKER_ORG/\2:latest#" \
            -e "s#([0-9.]+)=strimzi/([a-zA-Z0-9-]+:[a-zA-Z0-9.-]+-kafka-[0-9.]+)#\1=$DOCKER_ORG/\2#" \
            install/cluster-operator/050-Deployment-strimzi-cluster-operator.yaml
    ```

   This will update `050-Deployment-strimzi-cluster-operator.yaml` replacing all the image references (in `image` and `value` properties) with ones with the same name but with the repository changed.

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


