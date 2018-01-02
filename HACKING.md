# Building Strimzi

Strimzi is using `make` as its main build system. Our make build supports 
several different targets mainly for building and pushing Docker images.

<!-- TOC depthFrom:2 -->

- [Docker images](#docker-images)
    - [Building Docker images](#building-docker-images)
    - [Tagging and pushing Docker images](#tagging-and-pushing-docker-images)
- [Building everything](#building-everything)
- [Pushing images to the cluster's Docker repo](#pushing-images-to-the-clusters-docker-repo)
- [Release](#release)

<!-- /TOC -->

## Docker images

### Building Docker images

The `docker_build` target will build the Docker images provided by the 
Strimzi project. You can build all Strimzi Docker images by calling 
`make docker_build` from the root of the Strimzi repository. Or you can build 
an individual Docker image by running `make docker_build` from the 
subdirectories with their respective Dockerfiles - e.g. `kafka_base`, 
`kafka_statefulsets` etc.

The `docker_build` target will always build the images under the 
`strimzi` organization. This is necessary in order to be able to reuse 
the base image you might have just built without modifying all Dockerfiles. 
The `DOCKER_TAG` environment variable configures the Docker tag 
to use (default is `latest`).

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

`make all` command can be used to triger all the tasks above - build the 
Docker images, tag them and push them to the configured repository.

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

        docker login -u `oc project -q` -p `oc whoami -t` 172.30.1.1:5000
        
   `172.30.1.1:5000` happens to be the IP and port of the Docker registry
   running in the cluster (see `oc get svc -n default`). Note that the 
   Docker user is the OpenShift *project*, not the OpenShift user, and we're 
   using the token for the current (`developer`) login as the password. 
        
3. Now run `make` to push the development images to that Docker repo:

        DOCKER_REGISTRY=172.30.1.1:5000 DOCKER_ORG=`oc project -q` make all
        
4. Finally, when creating the new app from the template you need to
   specify the Docker repo, otherwise OpenShift will still try to pull 
   the image from docker.io:
   
        oc new-app strimzi -p IMAGE_REPO_NAME=172.30.1.1:5000/myproject


## Release

`make release` target can be used to create a release. Environment variable 
`RELEASE_VERSION` (default value `latest`) can be used to define the release 
version. The `release` target will create an archive with the Kubernetes and 
OpenShift YAML files which can be used for deployment. It will not build the 
Docker images - they should be built and pushed automatically by Travis CI 
when the release is tagged in the GitHub repsoitory.