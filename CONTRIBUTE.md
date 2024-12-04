# Contributing to Metabolic

## Install the tools
The following software is required to work with Metabolic codebase and run it locally:
- Git
- JDK
- SBT
- Spark

## GitHub account
Metabolic uses [GitHub](GitHub.com) for its primary code repository and for pull-requests, so if you don't already have a GitHub account you'll need to create one.

# Working with the codebase
## Fork the Metabolic repository
Go to the Metabolic repository and press the "Fork" button near the upper right corner of the page. When finished, you will have your own "fork" at https://github.com/<your-username>/core, and this is the repository to which you will upload your proposed changes and create pull requests.

## Clone your fork
At a terminal, go to the directory in which you want to place a local clone of the Metabolic repository, and run the following command to clone the repo:

git clone https://github.com/<your-username>/core.git

## Building locally
To build the source code locally, checkout and update the main branch:

    $ git checkout main

Then use SBT to compile everything, run all unit and integration tests, build all artifacts, and install all JAR, ZIP, and TAR files into your local Maven repository:

    $ sbt clean assembly

## Running and debugging tests
You can run all the predefined tests what are mandatory in the CI pipeline manually using SBT:

    $ sbt test

There is also an script that simulates the deployment of Metabolic, you can run it with your terminal:

    $ sh local.sh


## Creating a pull request
Once you're finished making your changes, your forked branch should have your commit(s) and you should have verified that your branch builds successfully. At this point, you can shared your proposed changes and create a pull request. To do this, first push your branch (and its commits) to your fork repository (called origin) on GitHub:

Then, in a browser go to your forked repository, and you should see a small section near the top of the page with a button labeled "Contribute". GitHub recognized that you pushed a new topic branch to your fork of the upstream repository, and it knows you probably want to create a pull request with those changes. Click on the button, and a button "Open pull request" will apper. Click it and GitHub will present you the "Comparing changes" page, where you can view all changes that you are about to submit. With all revised, click in "Create pull request" and a short form will be given, that you should fill out with information about your pull request. The title should start with the Jira issue and end with a short phrase that summarizes the changes included in the pull request. (If the pull request contains a single commit, GitHub will automatically prepopulate the title and description fields from the commit message.)

When completed, press the "Create" button and copy the URL to the new pull request.
