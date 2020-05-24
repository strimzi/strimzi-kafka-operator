# Documentation improvements

This proposal outlines possible improvements to or documentation

## Current situation

The documentation is written in [Asciidoc](http://asciidoc.org/) format and converted to HTML for publishing on the website using the [Asciidoctor](https://asciidoctor.org/) utility.

Our documentation is current following the so called modular approach:

* Everything is written in small modules / files
* These modules can have 3 different types:
    * Concept
    * Reference
    * Procedure
* Out of these, especially the procedure modules follow a predefined layout.
* These modules are than assembled in _assemblies_ which create the 4 different documentation books.
* All these modules are currently stored only in few directories

Across the documentation, different variables / placeholders are used which allow easily substituting.
These are used in place of things which are often changing such as versions.
But also on other places - for example instead of saying just `Strimzi`, the documentation uses the variable `{ProductName}`.

## Challenges

The current documentation as described above has many challenges and is not friendly to contributors.
As a result, most of the documentation work is done only by 2 people (@PaulRMellor and @laidan6000).
While they do a great job, this situation is not optimal and it would be much better to have:

* Bigger number of documentation contributors to better spread the load
* Have the documentation work directly part of the PR which is changing a feature or adding new feature instead of having it done asynchronously by someone else

This should also improve the release cycle which often has to wait for documentation for already merged features.
With the documentation being done together with the code, the documentation will be either merged with the feature or the feature will not be merged.

Apart form the way it influences the release cycle, the current documentation has also many other aspects which are not community friendly and which might discourage contributors:

### It is hard to write

* No tooling exists for working with the modular documentation.
Contributor needs to find and edit the right files among several hundred of modules.
There is no tool which would display the whole book and let the user to do changes to the individual modules from it.

* It is hard to see how the changes / modules fit together because to see them all combined you need to render the documentation using Asciidoctor, open it in browser, find the right place, etc. 
It is not just hard to find the files, the overall structure of the document is also very unclear from the edited files which makes it really hard to implement and changes or add new parts. 
Even a simple change of few words feels like a chore.

* Including single module many times is tempting and saves some effort. 
But it also makes it harder to update because it is not clear on how many different places this change will be actually done.

* The use of placeholders and variables for things such as the project name makes it hard to write the documentation since you need to know about these variables and keep them in mind all the time instead of focusing on the real content.

### It is hard to review

* It is not clear how the final document will look like and what will be the structure or order of the modules. 
Since there is currently no CI/CD system available, authors often have to provide rendered version of the document “out of band”.

* Some reviewers actually know how to render the docs to help with the review, but that is still only partially helpful because it does not show the changes. 
So it makes it easy to miss the changes etc. 
So you need to spend all the time looking across several windows and still risk seeing all the changes.

* As a result it happened several times that we were surprised by what the changes did later after they were merged.

### It is hard to read

* Modular documentation enforces a structure which is hard to read for the users. 
It prioritizes the structure over the value for the users. 
This is most obvious with the procedures which have their own structure which makes it sometimes hard to follow and makes even a procedure with one or two commands look big and complicated.

* The artificial split into procedures, concepts and references is also confusing for readers because they don’t always know that the procedure is a separate module from the concept and reference and they might need to look in other chapter or in other places.

* The enforced structure and re-use of modules often creates documentation which is hard to read and contains a lot of text which makes it just harder to read.

* An example of this is the Quickstart Guide / Evaluating guide. The series of included procedures has a repeating “Prerequisite” sections which really just describe the previous chapters of the same guide. 
The feedback we got on this is that this is very confusing and not clear to some users that they can ignore it etc.

## Things we should keep

Asciidoc as the language used for writing the documentation seems to work well for us.
We have existing integration using Asciidoctor.
There are also many other tools which we can use if we decide that the documentation should be presented in a different way such as for example Antora which can be used to render the docs form Markdown more in a _website style_ form.

Asciidoc was designed for writing documentation and supports rich syntax to achieve all the typography needed for a software documentation.
Its structure is easy to use and the basic are covered in our [contributing guide](https://strimzi.io/contributing/guide/) and are also easy to learn from the existing parts.

While Markdown might be slightly more widespread, it is sometimes too simple for things such as documentation.
As a result, it sometimes suffers from a lot of different flavors which add different missing features.
Also, to more to Markdown, we would need to change the tooling and migrate the whole existing documentation.
Keeping Asciidoc allows us to change the documentation format on the fly over longer period of time.

## Proposed improvements

This design document proposes following improvements to make the documentation better and easier to contribute:

* The placeholders / variables should be used only where it makes sense and adds value.
For example for parts of the text which are used on many places and are often changing such as versions of Kafka, Strimzi or their dependencies, or API versions.

* The placeholders or variables should not be used for text which is not changing and which makes witting harder.
For example `{ProductName}` instead of `Strimzi` or `{OAuth}` instead of `OAuth 2.0`.

* Instead of using small modules, the documentation should be composed from bigger files covering whole chapters or even whole books (for smaller books such as the overview or quickstart guides).
This should make it easier to write and review the documentation since it won't be split into hundred smaller files.

* Small modules should be used only for parts which are repeating very often across multiple books or for the parts which are generated from the build system such as list of supported Kafka versions etc.

* Procedures should follow the current template only where it makes sense and does not make the procedure hard to read. 
For example when the template is longer that the actual procedure or when the prerequisites in the same book right before given procedure.
Especially for shorter procedures, it might be easier to have the commands part of the concept or reference
For bigger procedures or 

* Documentation should be part of the PR which does the corresponding changes to the code.

* We should use more the API reference which is generated.
Description of the different options and how do they work should be ideally part of the API reference and not be in some other place in the book.
For longer texts, the `@DescirptionFile` annotation with a separate Asciidoc file can be used.

## Next steps

Should this proposal be approved, following changes should be done to the documentation immediately:
* Update the Contributing guide to reflect the new rules
* Remove the placeholders / variables we do not want to use anymore and replace them with the actual values. This should include for example `{ProductName}`.
* All new documentation from this point should be preferably written in non-modular fashion in bigger files corresponding to books or chapters.
    * The only exception would be small changes such as fixing typos.

Additionally, over the next moths and next releases we should work on gradually implementing the changes to the existing documentation:
* Join the small modules into bigger blocks corresponding to books or chapters
* Remove the _procedure structure_ in places where it makes no sense
* These changes could be done when some bigger changes are being done to the different sections of the documentation.
