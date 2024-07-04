# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.195.0/containers/java/.devcontainer/base.Dockerfile
# [Choice] Java version (use -bullseye variants on local arm64/Apple Silicon): 8, 11, 16, 8-bullseye, 11-bullseye, 16-bullseye, 8-buster, 11-buster, 16-buster
ARG VARIANT=11-bullseye
FROM mcr.microsoft.com/vscode/devcontainers/java:0-${VARIANT}

ARG MAVEN_VERSION=""
RUN su vscode -c "umask 0002 && . /usr/local/sdkman/bin/sdkman-init.sh && sdk install maven \"${MAVEN_VERSION}\"";
COPY --chown=vscode:vscode settings.xml /home/vscode/.m2/
COPY maven-settings.xml /usr/share/maven/ref/

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends git openssh-client less iproute2 procps curl lsb-release exa wget unzip shellcheck ruby-full

# Install helm
RUN curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 \
    && chmod 700 get_helm.sh \
    && ./get_helm.sh

# Install ASCIIDOCTOR
RUN gem install asciidoctor \
    && gem install asciidoctor-pdf

# Clean up
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Allow for a consistant java home location for settings - image is changing over time
RUN if [ ! -d "/docker-java-home" ]; then ln -s "${JAVA_HOME}" /docker-java-home; fi

# [Optional] Uncomment this line to install global node packages.
# RUN su vscode -c "source /usr/local/share/nvm/nvm.sh && npm install -g <your-package-here>" 2>&1

# make docu_html
# make java_build or make MVN_ARGS='-DskipTests -DskipITs' all or mvn install -DskipTests -DskipITs
# make docker_build

USER vscode
RUN curl -sS https://webi.sh/yq | sh