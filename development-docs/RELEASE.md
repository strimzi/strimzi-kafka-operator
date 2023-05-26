# Release

`make release` target can be used to create a release. Environment variable `RELEASE_VERSION` (default value `latest`) can be used to define the release version. The `release` target will: 
* Update all tags of Docker images to `RELEASE_VERSION` 
* Update documentation version to `RELEASE_VERSION` 
* Set version of the main Maven projects (`topic-operator` and `cluster-operator`) to `RELEASE_VERSION` 
* Create TAR.GZ and ZIP archives with the Kubernetes and OpenShift YAML files which can be used for deployment and documentation in HTML format.
 
The `release` target will not build the Docker images - they should be built and pushed automatically by Travis CI when the release is tagged in the GitHub repository. It also doesn't deploy the Java artifacts anywhere. They are only used to create the Docker images.

The release process should normally look like this:

1. Create a release branch
2. On the `main` git branch of the repository:
   * Update the versions to the next SNAPSHOT version using the `next_version` `make` target. For example to update the next version to `0.6.0-SNAPSHOT` run: `make NEXT_VERSION=0.6.0-SNAPSHOT next_version`.
   * Update the product version in `attributes.adoc` to the next version
   * Add a header for the new release to the `CHANGELOG.md` file

3. Run `make clean`
4. Export the desired version into the environment variable `RELEASE_VERSION`
   * Use always the GA version here (e.g. `0.6.0`) and not the RC version (`0.6.0-rc1`)
5. Run `make release`
   * This will automatically update all files in `packaging/` as well as in `install/`, `example/` and `helm-chart/`
6. Update the checksums for released files in `.checksums` in the release branch
   * Use the Make commands `make checksum_examples`, `make checksum_install`, and `make checksum_helm`
   * Updated the checksums in the `.checksums` file in the root directory of the GitHub repository
7. Commit the changes to the existing files (do not add the newly created top level TAR.GZ, ZIP archives or .yaml files into Git)
8. Push the changes to the release branch on GitHub
9. Wait for the CI to complete the build
   * Once it completes, mark the build in the Azure Pipelines UI to be retained forever
   * Download the build artifacts (Docs archives)
   * Copy the build ID (from the URL in Azure Pipelines)
10. Run the `operators-release` pipeline manually in Azure pipelines
    * Select the release branch from the list
    * Set the desired release version (e.g. `0.6.0-rc1` for RCs or `0.6.0` GA releases)
    * Set the release suffix as `0`
    * Set the build ID to the build ID from previous step (For GA, this should be the build ID used for the last RC since there should be no changes)
11. Once the release build is complete:
    * Download the release artifacts
    * Check the images pushed to Quay.io
    * Mark the build in the Azure Pipelines UI to be retained forever
12. Create a GitHub tag and release based on the release branch. Attach the release artifacts and docs as downloaded from the Azure pipelines.
    * For RCs, the tag should be named with the RC suffix, e.g. `0.6.0-rc1`
13. _(only for GA, not for RCs)_ Update the website
    * Update the `_redirects` file to make sure the `/install/latest` redirect points to the new release.
    * Update the `_data/releases.yaml` file to add new release
    * Update the documentation: 
      * Create new directories `docs/operators/<new-version>` and `docs/operators/<new-version>/full` in the website repository
      * Copy files from the operators repository `documentation/htmlnoheader` to `docs/operators/<new-version>` in the website repository
      * Copy files from the operators repository `documentation/html` to `docs/operators/<new-version>/full` in the website repository
      * Create new files `contributing.md`, `deploying.md`, `overview.md`, `quickstart.md`, and `using.md` in `docs/operators/<new-version>` - the content of these files should be the same as for older versions, so you can copy them and update the version number.
      * Delete the old HTML files and images from `docs/operators/latest` and `docs/operators/latest/full` (keep the `*.md` files) 
      * Copy files from the operators repository `documentation/htmlnoheader` to `docs/operators/latest` in the website repository
      * Copy files from the operators repository `documentation/html` to `docs/operators/latest/full` in the website repository
    * Add the Helm Chart repository `index.yaml` on our website:
      * Download the release artifacts from the CI pipeline and unpack them
      * Use the `helm` command to add the new version to the `index.yaml` file:
        ```
        helm repo index <PATH_TO_THE_DIRECTORY_WITH_THE_ARTIFCATS> --merge <PATH_TO_THE_INDEX_YAML> --url <URL_OF_THE_GITHUB_RELEASE_PAGE>
        ```
        For example, for Strimzi 0.34.0 release, if you unpacked the release artifacts to `./strimzi-0.34.0-rc1/` and have the Strimzi website checkout in `strimzi.github.io/`, you would run:
        ```
        helm repo index ./strimzi-0.34.0/ --merge ./strimzi.github.io/charts/index.yaml --url https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.34.0/
        ```
      * The updated `index.yaml` will be generated in the directory with the artifacts.
        Verify the added data and the digest and if they are correct, copy it to `charts/index.yaml` on the website. 

14. _(only for GA, not for RCs)_ On the `main` git branch of the repository:
    * Update the `ProductVersion` variable in `documentation/using/shared/attributes.adoc`
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
