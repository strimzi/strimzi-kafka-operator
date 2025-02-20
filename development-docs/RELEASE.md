# Release

`make release` target can be used to create a release. Environment variable `RELEASE_VERSION` (default value `latest`) can be used to define the release version. The `release` target will: 
* Update all tags of Docker images to `RELEASE_VERSION` 
* Update documentation version to `RELEASE_VERSION` 
* Set version of the main Maven projects (`topic-operator` and `cluster-operator`) to `RELEASE_VERSION` 
* Create TAR.GZ and ZIP archives with the Kubernetes and OpenShift YAML files which can be used for deployment and documentation in HTML format.
 
The `release` target will not build the Docker images - they should be built and pushed automatically by the Azure Pipelines when the release is tagged in the GitHub repository. It also doesn't deploy the Java artifacts anywhere. They are only used to create the Docker images.

The release process should normally look like this:

1. Create a release branch starting from the `main` one. The new release branch has to be named like `release-<Major>.<minor>.x`, for example `release-0.45.x` to be used for all patch releases for the 0.45.
2. On the `main` git branch of the repository:
   * Update the versions to the next SNAPSHOT version using the `next_version` `make` target. For example to update the next version to `0.46.0-SNAPSHOT` run: `NEXT_VERSION=0.46.0-SNAPSHOT make next_version`.
   * Update the product version in the `documentation/shared/attributes.adoc` file to the next version by setting the `ProductVersion` variable
   * Add a header for the new release to the `CHANGELOG.md` file
3. Move to the release branch and run `make clean`
4. Use the `RELEASE_VERSION` environment variable to set the desired version
   * Use always the GA version here (e.g. `0.45.0`) and not the RC version (e.g `0.45.0-rc1`)
5. Run `RELEASE_VERSION=<desired version> make release`, for example `RELEASE_VERSION=0.45.0 make release`
   * This will automatically update several `pom.xml` files and all files in `packaging/`, `install/`, `example/` and `helm-chart/` folders.
6. Update the checksums for released files in `.checksums` in the release branch
   * Use the Make commands `make checksum_examples`, `make checksum_install`, and `make checksum_helm` to generate the new checksums
   * Updated the checksums in the `.checksums` file in the root directory of the GitHub repository
7. Commit the changes to the existing files (do not add the newly created top level tar.gz, zip archives or .yaml files named like `strimzi-*` into Git)
8. Push the changes to the release branch on GitHub
9. Wait for the CI to complete the build
   * Once it completes, mark the build in the Azure Pipelines UI to be retained forever
   * Copy the build ID (from the URL in Azure Pipelines)
10. Run the `operators-release` pipeline manually in Azure pipelines
    * Select the release branch from the list
    * Set the desired release version (e.g. `0.45.0-rc1` for RCs or `0.45.0` GA releases)
    * Set the release suffix as `0`
    * Set the build ID to the build ID from previous step (For GA, this should be the build ID used for the last RC since there should be no changes)
11. Once the release build is complete:
    * Download the release artifacts (binary, documentation and sbom)
    * Check the images pushed to Quay.io
    * Mark the build in the Azure Pipelines UI to be retained forever
12. Create a GitHub tag and release based on the release branch. Attach the release artifacts and docs as downloaded from the Azure pipelines.
    * For RCs, the tag should be named with the RC suffix, e.g. `0.45.0-rc1`
13. _(only for GA, not for RCs)_ Update the website
    * Update the `_redirects` file to make sure the `/install/latest` redirect points to the new release.
    * Update the `_data/releases.yaml` file to add new release
    * Update the documentation: 
      * Create new directories `docs/operators/<new-version>` and `docs/operators/<new-version>/full` in the website repository
      * Copy files from the operators repository `documentation/htmlnoheader` to `docs/operators/<new-version>` in the website repository
      * Copy files from the operators repository `documentation/html` to `docs/operators/<new-version>/full` in the website repository
      * Create new files `configuring.md`, `deploying.md` and `overview.md` in `docs/operators/<new-version>` - the content of these files should be the same as for older versions, so you can copy them and update the version number.
      * Delete the old HTML files and images from `docs/operators/latest` and `docs/operators/latest/full` (keep the `*.md` files) 
      * Copy files from the operators repository `documentation/htmlnoheader` to `docs/operators/latest` in the website repository
      * Copy files from the operators repository `documentation/html` to `docs/operators/latest/full` in the website repository
    * Add the Helm Chart repository `index.yaml` on our website:
      * Download the release artifacts from the CI pipeline and unpack them
      * Use the `helm` command to add the new version to the `index.yaml` file:
        ```
        helm repo index <PATH_TO_THE_DIRECTORY_WITH_THE_ARTIFACTS> --merge <PATH_TO_THE_INDEX_YAML> --url <URL_OF_THE_GITHUB_RELEASE_PAGE>
        ```
        For example, for Strimzi 0.45.0 release, if you unpacked the release artifacts to `./strimzi-0.45.0-rc1/` and have the Strimzi website checkout in `strimzi.github.io/`, you would run:
        ```
        helm repo index ./strimzi-0.45.0/ --merge ./strimzi.github.io/charts/index.yaml --url https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.45.0/
        ```
      * The updated `index.yaml` will be generated in the directory with the artifacts.
        Verify the added data and the digest and if they are correct, copy it to `charts/index.yaml` on the website. 

14. _(only for GA, not for RCs)_ On the `main` git branch of the repository:
    * Update the `ProductVersion` variable in `documentation/shared/attributes.adoc`
    * Update the `install`, `examples` and `helm-chart` directories in the `main` branch with the newly released files
    * Update the checksums for released files in `.checksums`

15. _(only for GA, not for RCs)_ The maven artifacts (`api` module) will be automatically staged from Azure during the tag build. It has to be releases from [Sonatype](https://oss.sonatype.org/#stagingRepositories) to get to the main Maven repositories.
16. _(only for GA, not for RCs)_ Update the Strimzi manifest files in Operator Hub [community operators](https://github.com/operator-framework/community-operators) repository and submit a pull request upstream. *Note*: Instructions for this step need updating.
17. _(only for GA, not for RCs)_ Add the new version to the `systemtest/src/test/resources/upgrade/BundleUpgrade.yaml` file for the upgrade tests
18. _(only for GA, not for RCs)_ Add the new version to the `systemtest/src/test/resources/upgrade/BundleDowngrade.yaml` file and remove the old one for the downgrade tests

## Updating Kafka Bridge version

The version of Strimzi Kafka Bridge is defined in the file `./bridge.version`.
Even the `main` branch is using this fixed version and not the version build from the `main` branch of Kafka Bridge.
If you need to update the Kafka bridge to newer version, you should do it with following steps:

1. Edit the `bridge.version` file and update it to contain the new Bridge version
2. Run `make bridge_version` to update the related files to the new version
3. Commit all modified files to Git and open a PR.

## Running Azure System Tests pipelines

After releasing a RC, we need to run the following System Tests pipelines:

* helm-acceptance
* upgrade
* regression (multiple times, one for each supported Kafka version)
* feature-gates-regression (multiple times, one for each supported Kafka version)

## Rebuild container image for base image CVEs

Overtime, the base container image could be affected by CVEs related to the installed JVM, operating system libraries and so on.
Security issues are usually reported by security scanner tools used by the community users as well as project contributors.
The Quay.io registry also runs such scans periodically to look for security issues reported on the website.
Checking the Quay.io website is a way to get the status of security vulnerabilities affecting the operator container image.
In this case, we might need to rebuild the operator container image.
This can be done by using the `operators-cve-rebuild` pipeline.
This pipeline will take a previously built binaries and use them to build a new container image, which is then pushed to the container registry with the suffixed tag (e.g. `0.45.0-2`).
The suffix can be specified when starting the re-build pipeline.
You should always check what was the previous suffix and increment it.

When starting the pipeline, it will ask for several parameters which you need to fill:

* Release version (for example `0.45.0`)
* Release suffix (for example `2` - it is used to create the suffixed images such as `strimzi/operator:0.45.0-2` to identify different builds done for different CVEs)
* Source pipeline ID (Currently, only the build pipeline with ID `16` can be used)
* Source build ID (the ID of the build from which the artifacts should be used - use the long build ID from the URL and not the shorter build number). 
  You can also get the build ID by referring to the latest run of the corresponding release pipeline.

After pushing the suffixed tag image, the older images will be still available in the container registry under their own suffixes.
Only the latest rebuild will be available under the un-suffixed tag (for example, the `0.45.0` tagged image is still the previous one and not up to date with the CVEs respin).

Afterwards, it will wait for a manual approval with a timeout of 3 days (configured in the pipeline YAML).
This gives additional time to manually test the new container image.
After the manual approval, the image will be also pushed under the tag without suffix (e.g. `0.45.0`).

This process should be used only for CVEs in the base images.
Any CVEs in our code or in the Java dependencies require new patch (or minor) release.

## Operators Catalog

In order to make the Strimzi operator available in the [OperatorHub.io](https://operatorhub.io/) catalog, you need to build a bundle containing the Strimzi operator metadata together with its Custom Resource Definitions.
The metadata are described through a `ClusterServiceVersion` (CSV) resource declared by using a corresponding YAML file.

The bundle for the OperatorHub.io is available in the [Community-Operators](https://github.com/k8s-operatorhub/community-operators/tree/main/operators/strimzi-kafka-operator) GitHub repo.

In order provide the bundle for a new release, you can use the previous one as a base.  
Create a folder for the new release by copying the previous one and make the following changes:

* if releasing a new minor or major version (rather than fix), change the `metadata/annotations.yaml` to update the second channel listed next to `operators.operatorframework.io.bundle.channels.v1` to the new release version range (e.g. `strimzi-0.45.x`).
* copy the CRDs and the Cluster Roles YAML to the `manifests` folder by taking them from the `install/cluster-operator` folder (within the Strimzi repo).
* take the `strimzi-cluster-operator.v<VERSION>.clusterserviceversion.yaml` CSV file (by using the new release as `<VERSION>`) in order to update the following:
  * `metadata.annotations.alm-examples-metadata` section by using the examples from the `examples` folder (within the Strimzi repo).
  * `containerImage` field with the new operator image (using the SHA).
  * `name` field by setting the new version in the operator name.
  * `customresourcedefinitions.owned` section with the CRDs descriptions, from the `install/cluster-operator` folder (within the Strimzi repo).
  * `description` section with all the Strimzi operator information already used for the release on GitHub.
  * `install.spec.permissions` section by using the Cluster Role files from the `install/cluster-operator` (within the Strimzi repo).
  * `deployments` section by using the Strimzi Cluster Operator Deployment YAML from the `install/cluster-operator` (within the Strimzi repo) but using the SHAs for the images.
  * `relatedImages` section with the same images as the step before.
  * `replaces` field by setting the old version that this new one is going to replace.
  * `version` field with the new release.

After making all these changes, you can double-check the validity of the CSV by copy/paste its content into the [OperatorHub.io preview](https://operatorhub.io/preview) tool.

### Testing the bundle

When the operator manifests and metadata files are ready, you should test the operator bundle on an actual Kubernetes and OpenShift cluster.
If you are using Kubernetes, then you first need to install the Operator Lifecycle Manager (OLM) on it:

```shell
curl -sL https://github.com/operator-framework/operator-lifecycle-manager/releases/download/v0.31.0/install.sh | bash -s v0.31.0
```

The above step is not necessary if you are using OpenShift, because it has the OLM out-of-box.

In this section, the following steps show how to:

* create the operator bundle image and put it into a catalog image.
* publish the catalog on the cluster.
* install the operator from the catalog itself.

Prerequisites:

* [opm](https://github.com/operator-framework/operator-registry/releases) tool.
* docker, podman or buildah (the following steps will use docker as an example)

*Note*
For further details about the following steps, you can also refer to the official [Operator Lifecycle Manager documentation](https://olm.operatorframework.io/docs/tasks/).

#### Create the operator bundle image

In this step, you are going to create a container image containing the bundle with the operator manifests and metadata. 
Inside the bundle directory (i.e. `operators/strimzi-kafka-operator/0.45.0`), export the following environment variables to specify the operator version and the container registry and user used to push the bundle and catalog images:

``shell
export OPERATOR_VERSION=0.45.0
export DOCKER_REGISTRY=quay.io
export DOCKER_USER=ppatierno
``

Run the following command in order to generate a `bundle.Dockerfile` representing the operator bundle image.

```shell
opm alpha bundle generate --directory ./manifests/ --package strimzi-kafka-operator --channels stable,strimzi-0.45.x --default stable
```

*Note*
The channels are the same as the ones specified in the `metadata/annotations.yaml` file.

The generated Dockerfile will look like the following:

```dockerfile
FROM scratch

LABEL operators.operatorframework.io.bundle.mediatype.v1=registry+v1
LABEL operators.operatorframework.io.bundle.manifests.v1=manifests/
LABEL operators.operatorframework.io.bundle.metadata.v1=metadata/
LABEL operators.operatorframework.io.bundle.package.v1=strimzi-kafka-operator
LABEL operators.operatorframework.io.bundle.channels.v1=stable,strimzi-0.45.x
LABEL operators.operatorframework.io.bundle.channel.default.v1=stable

COPY manifests /manifests/
COPY metadata /metadata/
```

Run the following command to build the operator bundle image and push it to a repository:

```shell
docker build -t $DOCKER_REGISTRY/$DOCKER_USER/strimzi-kafka-operator-bundle:$OPERATOR_VERSION -f bundle.Dockerfile .
docker push $DOCKER_REGISTRY/$DOCKER_USER/strimzi-kafka-operator-bundle:$OPERATOR_VERSION
```

You can also run some validations to ensure that your bundle is valid and in the correct format.:

```shell
opm alpha bundle validate --tag $DOCKER_REGISTRY/$DOCKER_USER/strimzi-kafka-operator-bundle:$OPERATOR_VERSION --image-builder docker
```

#### Create the catalog image

In this step, you are going to create a catalog and put the operator bundle into it.
Inside the bundle directory (i.e. `operators/strimzi-kafka-operator/0.45.0`), create a folder for the catalog:

```shell
mkdir -p strimzi-catalog
```

Run the following command in order to generate a `strimzi-catalog.Dockerfile` representing the catalog image.

```shell
opm generate dockerfile strimzi-catalog
```

Initialize the catalog:

```shell
opm init strimzi-kafka-operator --default-channel=stable --output yaml > strimzi-catalog/index.yaml
```

Add the bundle to the catalog (repeat for each operator version/bundle you want to add in order to be able to test operator upgrades):

```shell
opm render $DOCKER_REGISTRY/$DOCKER_USER/strimzi-kafka-operator-bundle:$OPERATOR_VERSION --output=yaml >> strimzi-catalog/index.yaml
```

Edit the `index.yaml` to add the bundle to a channel (i.e. using the stable one):

```shell
cat << EOF >> strimzi-catalog/index.yaml
---
schema: olm.channel
package: strimzi-kafka-operator
name: stable
entries:
  - name: strimzi-cluster-operator.v0.45.0
EOF
```

Build and push the catalog image:

```shell
docker build -f strimzi-catalog.Dockerfile -t $DOCKER_REGISTRY/$DOCKER_USER/olm-catalog:latest .
docker push $DOCKER_REGISTRY/$DOCKER_USER/olm-catalog:latest
```

### Make the catalog available on cluster

Create a `CatalogSource` resource to make the catalog available on the cluster:

```shell
kubectl apply -f - <<EOF
apiVersion: operators.coreos.com/v1alpha1
kind: CatalogSource
metadata:
  name: strimzi-catalog
  namespace: olm # use openshift-marketplace if running on OpenShift
spec:
  sourceType: grpc
  image: quay.io/ppatierno/olm-catalog:latest # use the correct image
  displayName: Strimzi Catalog
EOF
```

You can now list the operator as part of the "Strimzi Catalog" catalog and not the "Community Operators" catalog (coming from OLM):

```shell
kubectl get packagemanifest -n olm | grep strimzi-kafka-operator
```

### Install the operator

Create a `kafka` namespace where the operator will be installed.
On Kubernetes, you can install the operator by creating an `OperatorGroup` and a `Subscription`:

```shell
kubectl apply -f - <<EOF
apiVersion: operators.coreos.com/v1
kind: OperatorGroup
metadata:
  name: strimzi-group
  namespace: kafka
EOF
```

```shell
kubectl apply -f - <<EOF
apiVersion: operators.coreos.com/v1alpha1
kind: Subscription
metadata:
  name: strimzi-subscription
  namespace: kafka
spec:
  channel: stable
  name: strimzi-kafka-operator
  source: strimzi-catalog
  sourceNamespace: olm
  installPlanApproval: Automatic
EOF
```

When the `Subscription` is created the OLM will use the "Strimzi Catalog" to install the operator from there.

On OpenShift, after the `CatalogSource` creation, you can use the web interface and install the operator from the Operator Catalog page.
There is no need to create an `OperatorGroup` and `Subscription` manually.

You can now deploy a Kafka cluster by using the installed operator and running some smoke tests.

Finally, uninstall the operator by deleting the `Subscription` and the corresponding `ClusterServiceVersion`:

```shell
CSV=$(kubectl get subscription strimzi-subscription -n kafka -o json | jq -r '.status.installedCSV')
kubectl delete subscription strimzi-subscription -n kafka
kubectl delete csv $CSV -n kafka
```

### Publishing to OperatorHub.io and OpenShift Operator Catalog

When the bundle was successfully tested, you can finally open a PR against the `k8s-operatorhub/community-operators` repository.
The PR will run some sanity checks which could need some fixes in case of errors.
After being reviewed by maintainers and merged, the Strimzi operator will be available in the OperatorHub.io website.

The operator should be made available in the OpenShift Operator Catalog as well.
The bundle for the OpenShift Operator Catalog is available in the https://github.com/redhat-openshift-ecosystem/community-operators-prod/tree/main/operators/strimzi-kafka-operator GitHub repo.
Just follow the same steps as for OperatorHub.io for building the bundle.