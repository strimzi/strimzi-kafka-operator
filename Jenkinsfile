@Library("jenkins-library@main")

import com.logicalclocks.jenkins.k8s.ImageBuilder

pipeline {
  agent { label 'local' }

  stages {
        stage('Clone repository') {
            steps {
                checkout scm
            }
        }
        stage("Java install strimzi") {
            agent {
                docker {
                    image 'maven:3.8.5-openjdk-17-slim'
                    args '--user=root -v $HOME/.m2:/root/.m2'
                }
            }
            steps {
                // Install dependencies
                sh '''
                    apt-get update && apt-get install -y make git zip
                '''

                // Install docker
                sh '''
                    curl -fsSL https://get.docker.com -o get-docker.sh
                    sh get-docker.sh
                '''

                // Install shellcheck for shell script linting
                sh '''
                    curl -L https://github.com/koalaman/shellcheck/releases/download/v0.9.0/shellcheck-v0.9.0.linux.x86_64.tar.xz | tar -xJ
                    cp shellcheck-v0.9.0/shellcheck /usr/local/bin/
                    chmod +x /usr/local/bin/shellcheck
                '''

                // Install yq for processing YAML files
                sh '''
                    curl -L https://github.com/mikefarah/yq/releases/download/v4.43.1/yq_linux_amd64 -o /usr/local/bin/yq
                    chmod +x /usr/local/bin/yq
                    yq --version
                '''

                // Install helm
                sh '''
                    curl -fsSL -o helm.tar.gz https://get.helm.sh/helm-v3.14.0-linux-amd64.tar.gz
                    tar -xzf helm.tar.gz
                    mv linux-amd64/helm /usr/local/bin/helm
                    chmod +x /usr/local/bin/helm
                '''

                // Get the authorizer (TODO: use existing jar when authorizer pr is merged)
                sh '''
                    rm -rf hops-kafka-authorizer
                    git clone --branch HWORKS-2215 --single-branch https://github.com/bubriks/hops-kafka-authorizer.git
                '''

                dir('hops-kafka-authorizer') {
                    sh 'mvn clean install'
                }

                // get kafka authorizer
                // sh "curl -L -o /tmp/hops-kafka-authorizer.jar https://repo.hops.works/master/hops-kafka-authorizer/4.0.0-SNAPSHOT/hops-kafka-authorizer-4.0.0-SNAPSHOT.jar"

                // Java build
                sh '''
                    make clean
                    make MVN_ARGS='-DskipTests' java_install
                '''

                stash name: 'docker-images', includes: 'docker-images/**'
            }
        }
        stage('Build and push images') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'a0770738-4ef3-4acc-a6ba-097ee6c85b44', passwordVariable: 'PASSWORD', usernameVariable: 'USERNAME')]) {
                    script {
                        unstash 'docker-images'

                        def version = readFile("release.version").trim()

                        // Build the Docker image
                        sh '''
                            make docker_build
                        '''

                        def builder = new ImageBuilder(this)

                        // Push the Docker image
                        builder.REGISTRIES.each { reg ->
                            // Authenticate to the registry
                            reg.auth()

                            // Get registryUrl
                            def fullImage = reg.buildImageName("dummy", "latest")
                            def registryUrl = fullImage.split('/')[0]

                            // Push the image
                            sh """
                                export DOCKER_REGISTRY=${registryUrl}
                                export DOCKER_ORG=strimzi
                                export DOCKER_TAG=${version}
                                make docker_push
                            """
                        }
                    }
                }
            }
        }
    }
}
