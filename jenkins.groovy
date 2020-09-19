/**
 * Download oc origin
 */
def downloadOcOrigin() {
    def originDownloadUrl = "https://github.com/openshift/origin/releases/download/v3.11.0/openshift-origin-client-tools-v3.11.0-0cbc58b-linux-64bit.tar.gz"
    sh(script: "rm -rf ~/.kube")
    sh(script: "mkdir -p /tmp/openshift")

    timeout(time: 5, unit: 'MINUTES') {
        status = sh(
                script: "wget $originDownloadUrl -O openshift.tar.gz -q",
                returnStatus: true
        )
    }
    //////////////////////////////////////////////////

    sh(script: "tar xzf openshift.tar.gz -C /tmp/openshift --strip-components 1")
    sh(script: "sudo cp /tmp/openshift/oc /usr/bin/oc")
    sh(script: "sudo cp /tmp/openshift/kubectl /usr/bin/kubectl")
    sh(script: "sudo rm -rf /tmp/openshift/")
    sh(script: "sudo rm -rf openshift.tar.gz")
}

def downloadCRC() {
    downloadOcOrigin()
    def crcBundleUrl = "https://mirror.openshift.com/pub/openshift-v4/clients/crc/1.6.0/crc-linux-amd64.tar.xz"
    withCredentials([string(credentialsId: 'crc-secret', variable: 'secret')]) {
        sh(script: "echo \'${secret}\' > ${WORKSPACE}/crcSecret")
    }
    sh(script: "mkdir -p /tmp/crc")

    timeout(time: 40, unit: 'MINUTES') {
        status = sh(
                script: "wget ${crcBundleUrl} -O crc.tar.xz -q",
                returnStatus: true
        )
    }
    //////////////////////////////////////////////////

    sh(script: "tar xvf crc.tar.xz -C /tmp/crc --strip-components 1")
    sh(script: "sudo cp /tmp/crc/crc /usr/bin/crc")
    sh(script: "sudo rm -rf /tmp/crc/")
    sh(script: "sudo rm -rf crc.tar.xz")
}

def createUserCRC(String url, String kubeadmPasswd) {
    println("Create user in on CRC cluster admin")
    login(url, "kubeadmin", kubeadmPasswd)

    sh(script: 'echo \'admin:$2y$05$e4L09O0hldfkIi8iKa0bV.Ai/HePB.fOHJRsYfZxOhkbtUR.oeKsW\' > users.htpasswd')
    sh(script: 'echo \'user:$2y$05$EuxM25KPffYEGop7Fo7kaeet3/AxRjpvpXVDVnUVz7JnGWO3AxrDe\' >> users.htpasswd')

    sh(script: 'echo "apiVersion: config.openshift.io/v1" > auth.yaml')
    sh(script: 'echo "kind: OAuth" >> auth.yaml')
    sh(script: 'echo "metadata:" >> auth.yaml')
    sh(script: 'echo "  name: cluster" >> auth.yaml')
    sh(script: 'echo "spec:" >> auth.yaml')
    sh(script: 'echo "  identityProviders:" >> auth.yaml')
    sh(script: 'echo "  - name: htpasswd" >> auth.yaml')
    sh(script: 'echo "    mappingMethod: claim" >> auth.yaml')
    sh(script: 'echo "    type: HTPasswd" >> auth.yaml')
    sh(script: 'echo "    htpasswd:" >> auth.yaml')
    sh(script: 'echo "      fileData:" >> auth.yaml')
    sh(script: 'echo "        name: htpass-secret-custom" >> auth.yaml')

    sh(script: "oc create secret generic htpass-secret-custom --from-file=htpasswd=./users.htpasswd -n openshift-config")
    sh(script: "oc apply -f auth.yaml")
    sh(script: "oc adm policy add-cluster-role-to-user cluster-admin admin --rolebinding-name=cluster-admin")
}

def startCRC() {
    sh(script: "sudo yum -y install NetworkManager")
    downloadCRC()
    sh(script: "crc setup")
    println("Run crc start")
    sh(script: "crc start -m 24200 -p \"${WORKSPACE}/crcSecret\"")
    def kubeadmPasswd = sh(script: "crc console --credentials | awk 'NR==2' | awk 'match(\$0,/[a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+-[a-zA-Z0-9]+/) {print substr(\$0,RSTART,RLENGTH)}'", returnStdout: true).trim()
    sh(script: "sleep 120")
    createUserCRC("https://api.crc.testing:6443", kubeadmPasswd)
    login("https://api.crc.testing:6443", "${env.TEST_CLUSTER_ADMIN}", "${env.TEST_CLUSTER_ADMIN}")
}


/**
 * Prepare and start origin environment.
 */
def startOrigin() {
    downloadOcOrigin()
    timeout(time: 15, unit: 'MINUTES') {
        status = sh(
                script: "oc cluster up --public-hostname=\$(hostname -I | awk '{print \$1}') --base-dir ${ORIGIN_BASE_DIR} --enable=*,service-catalog,web-console --insecure-skip-tls-verify=true",
                returnStatus: true
        )
    }
    def url = sh(script: "echo \"\$(hostname -I | awk '{print \$1}')\"", returnStdout: true).trim()
    createUserOrigin("${env.TEST_CLUSTER_ADMIN}", "https://${url}:8443")
    login("https://${url}:8443", "${env.TEST_CLUSTER_ADMIN}", "${env.TEST_CLUSTER_ADMIN}")
    fixPVOnOrigin()
}

/**
 * Login kubernetes user
 * @param url url to cluster
 * @param user username
 * @param pass password
 */
def login(String url, String user, String pass) {
    for (i = 0; i < 10; i++) {
        println("[INFO] Logging in on OCP ${url} as ${user}")
        def status = sh(script: "oc login -u \"${user}\" -p \"${pass}\" --insecure-skip-tls-verify=true ${url}", returnStatus: true)
        if (status != 0) {
            echo "[WARN] Login failed, waiting 1 minute before next try"
            sh(script: "sleep 60")
        } else {
            break
        }
    }
}

/**
 * create user in cluster and set him as cluster-admin
 * @param username
 * @param url
 */
def createUserOrigin(String username, String url) {
    println("Create user in on OCP ${username}")
    sh(script: "oc login -u system:admin --insecure-skip-tls-verify=true --config=${KUBE_CONFIG_PATH} ${url}")
    sh(script: "oc adm policy add-cluster-role-to-user cluster-admin ${username} --rolebinding-name=cluster-admin --config=${KUBE_CONFIG_PATH}")
    sh(script: "oc label node localhost rack-key=zone --config=${KUBE_CONFIG_PATH}")
}

def fixPVOnOrigin() {
    sh(script: "oc get pv")
    sh(script: "oc adm policy add-scc-to-group hostmount-anyuid system:serviceaccounts")
}

/**
 * Function for teardown the test cluster.
 */
def teardownEnvironment(String workspace) {
    def status = sh(
            script: "oc cluster down",
            returnStatus: true
    )

    if (status != 0) {
        echo "OpenShift failed to stop"
    }

    sh "for i in \$(mount | grep openshift | awk '{ print \$3}'); do sudo umount \"\$i\"; done && sudo rm -rf ${workspace}/origin"
    sh "sudo rm -rf ${workspace}/origin/"
}

def clearImages() {
    sh "docker rmi -f \$(docker images -q) 2>/dev/null || echo 'No more images to remove.'"
}

def installHelm(String workspace) {
    sh(script: "${workspace}/.travis/setup-helm.sh")
}

def installYq(String workspace) {
    sh(script: "${workspace}/.travis/install_yq.sh")
}

def buildStrimziImages() {
    sh(script: "MVN_ARGS='-Dsurefire.rerunFailingTestsCount=5 -Dfailsafe.rerunFailingTestsCount=2' make docker_build")
    sh(script: "make docker_tag")
    // Login to internal registries
    sh(script: "docker login ${env.DOCKER_REGISTRY} -u \$(oc whoami) -p \$(oc whoami -t)")
    // Create namespace for images
    sh(script: "oc create namespace ${env.DOCKER_ORG}")
    sh(script: "make docker_push")
}

def runSystemTests(String workspace, String testCases, String testProfile, String excludeGroups) {
    withMaven(mavenOpts: '-Djansi.force=true') {
        sh "mvn -f ${workspace}/systemtest/pom.xml -P all verify -Dgroups=${testProfile} -DexcludedGroups=${excludeGroups} -Dit.test=${testCases} -Djava.net.preferIPv4Stack=true -DtrimStackTrace=false -Dstyle.color=always --no-transfer-progress -Dfailsafe.rerunFailingTestsCount=2"
    }
}

def postAction(String artifactDir, String prID, String prAuthor, String prTitle, String prUrl, String buildUrl, String workspace, String address) {
    def status = currentBuild.result
    //store test results from build and system tests
    junit testResults: '**/TEST-*.xml', allowEmptyResults: true
    //archive test results and openshift logs
    archive '**/TEST-*.xml'
    try {
        archive "${artifactDir}/**"
    } catch(all) {
        echo "Archive failed"
    } finally {
        echo "Artifacts are stored"
    }
    if (status == null) {
        currentBuild.result = 'SUCCESS'
        sendMail(address, "succeeded", prID, prAuthor, prTitle, prUrl, buildUrl)
    }
    teardownEnvironment(workspace)
}

def sendMail(String address, String status, String prID, String prAuthor, String prTitle, String prUrl, String buildUrl) {
    mail to:"${address}", subject:"Build of Strimzi PR#${prID} by ${prAuthor} - '${prTitle}' has ${status}", body:"PR link: ${prUrl}\nBuild link: ${buildUrl}"
}

def postGithubPrComment(def file) {
    echo "Posting github comment"
    echo "Going to run curl command"
    withCredentials([string(credentialsId: 'strimzi-ci-github-token', variable: 'GITHUB_TOKEN')]) {
        sh "curl -v -H \"Authorization: token ${GITHUB_TOKEN}\" -X POST -H \"Content-type: application/json\" -d \"@${file}\" \"https://api.github.com/repos/Strimzi/strimzi-kafka-operator/issues/${ghprbPullId}/comments\" > out.log 2> out.err"
        def output=readFile("out.log").trim()
        def output_err=readFile("out.err").trim()
        echo "curl output=$output output_err=$output_err"
    }
}

return this
