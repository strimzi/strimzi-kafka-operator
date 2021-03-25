# Release

`make release` target can be used to create a release. Environment variable `RELEASE_VERSION` (default value `latest`) can be used to define the release version. The `release` target will: 
* Update all tags of Docker images to `RELEASE_VERSION` 
* Update documentation version to `RELEASE_VERSION` 
* Set version of the main Maven projects (`topic-operator` and `cluster-operator`) to `RELEASE_VERSION` 
* Create TAR.GZ and ZIP archives with the Kubernetes and OpenShift YAML files which can be used for deployment and documentation in HTML format.
 
The `release` target will not build the Docker images - they should be built and pushed automatically by Travis CI when the release is tagged in the GitHub repository. It also doesn't deploy the Java artifacts anywhere. They are only used to create the Docker images.

The release process should normally look like this:

1. Create a release branch
2. On the `master` git branch of the operators repository:
  * Update the versions to the next SNAPSHOT version using the `next_version` `make` target. For example to update the next version to `0.6.0-SNAPSHOT` run: `make NEXT_VERSION=0.6.0-SNAPSHOT next_version`.

3. Run `make clean`
4. Export the desired version into the environment variable `RELEASE_VERSION`
5. Run `make release`
6. Update the `install`, `example` and `helm-chart` files in the release branch with files from `packaging/` updated by `make release`
7. Commit the changes to the existing files (do not add the newly created top level TAR.GZ, ZIP archives or .yaml files into Git)
8. Push the changes to the release branch on GitHub
9. Create the tag and push it to GitHub. Tag name determines the tag of the resulting Docker images. Therefore the Git tag name has to be the same as the `RELEASE_VERSION`, i.e. `git tag ${RELEASE_VERSION}`,
10. Once the CI build for the tag is finished, and the Docker images are pushed to Docker Hub, Create a GitHub release and tag based on the release branch. Attach the TAR.GZ/ZIP archives, YAML files (for installation from URL) from step 5 and the Helm Chart to the release
11. _(only for GA, not for RCs)_ Update the website
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
  * Update the Helm Chart repository file by copying `helm-charts/helm3/index.yaml` in the operators GitHub repository to `charts/index.yaml` in the website GitHub repsoitory.

12. _(only for GA, not for RCs)_ The maven artifacts (`api` module) will be automatically staged from Azure during the tag build. It has to be releases from [Sonatype](https://oss.sonatype.org/#stagingRepositories) to get to the main Maven repositories.
13. _(only for GA, not for RCs)_ On the `master` git branch of the operators repository:
  * Copy the `packaging/helm-charts/index.yaml` from the `release` branch to `master`
  * Update the `ProductVersion` variable in `documentation/using/shared/attributes.doc`
  * Update the `install`, `examples` and `helm-chart` directories in the `master` branch with the newly released files

14. _(only for GA, not for RCs)_ Update the Strimzi manifest files in Operator Hub [community operators](https://github.com/operator-framework/community-operators) repository and submit a pull request upstream. *Note*: Instructions for this step need updating.
15. _(only for GA, not for RCs)_ Add the new version to the `systemtest/src/test/resources/upgrade/StrimziUpgradeST.json` file for the upgrade tests
16. _(only for GA, not for RCs)_ Add the new version to the `systemtest/src/test/resources/upgrade/StrimziDowngradeST.json` file and remove the old one for the downgrade tests


## Updating Kafka Bridge version

The version of Strimzi Kafka Bridge is defined in the file `./bridge.version`.
Even the master branch is using this fixed version and not the version build from the `master` branch of Kafka Bridge.
If you need to update the Kafka bridge to newer version, you should do it with following steps:

1. Edit the `bridge.version` file and update it to contain the new Bridge version
2. Run `make bridge_version` to update the related files to the new version
3. Commit all modified files to Git and open a PR.
