[strimziDoc]: https://strimzi.io/documentation/ "Strimzi documentation"
[strimziContrib]: https://strimzi.io/contributing/guide/ "Strimzi documentation contributor guide"

<!-- omit from toc -->
# Strimzi documentation

Welcome to the Strimzi documentation! 

This folder contains the source files for managing the Strimzi documentation. The documentation is written in AsciiDoc, and it covers guides to help you understand, deploy, and configure Strimzi.

<!-- omit from toc -->
## Table of Contents
- [Strimzi guides](#strimzi-guides)
- [Documentation folder structure](#documentation-folder-structure)
- [Contributing to the documentation](#contributing-to-the-documentation)

## Strimzi guides

The Strimzi documentation is organized into the following guides:

- **Overview guide**
  - Files: [/documentation/overview/](overview)
  - This guide provides an overview of Strimzi, explaining its key concepts and features.

- **Deploying and Managing Strimzi guide**
  - Files: [/documentation/deploying/](deploying)
  - This guide provides instructions and best practices for deploying and managing Strimzi.

- **API Reference**
  - Files: [/documentation/configuring/](configuring)
  - This guide provides detailed information on configuring Strimzi through its API.

## Documentation folder structure

The content for each Strimzi guide is encapsulated in a main source file:

* `overview/overview.adoc` - Overview guide
* `deploying/deploying.adoc` - Deploying and Managing Strimzi guide
* `configuring/configuring.adoc` - API Reference

The main source files are used to build the documentation and are contained in separate folders.
Other documentation folders contain the content that's incorporated into the main source files.

**Documentation folders**
| Folder                   | Description                                          |
| --------------           | -------------------------------------------------    |
| `api/`                   | Property descriptions for the API Reference guide    |
| `contributing/`          | Documentation Contributor Guide                      |
| `deploying/`             | Deploying and Managing Strimzi guide (main)          |
| `overview/`              | Strimzi Overview guide (main)                        |
| `configuring/`           | API Reference (main)                                 |
| `assemblies/`            | Assemblies (chapters) provide content for all guides |
| `modules/`               | Modules provide content for assemblies               |
| `shared/`                | Shared include files                                 |
| `shared/attributes.adoc` | Global book attributes                               |
| `shared/images/`         | Shared image files                                   |

## Contributing to the documentation

If there's something that you want to add or change in the documentation, do the following:

1. Set up a local Git repository
2. Create a branch for your changes
3. Add the changes through a pull request

The pull request will be reviewed and the changes merged when the review is complete.
The guide is then rebuilt and the updated content is published on the Strimzi website.
Published documentation for the current _main_ branch as well as all releases can be found on our [website][strimziDoc].

For more information on setting up and contributing to the documentation, see the [Strimzi documentation contributor guide][strimziContrib].



