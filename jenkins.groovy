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
    sh(script: "sudo rm -rf /tmp/openshift/")
    sh(script: "sudo rm -rf openshift.tar.gz")
}


/**
 * Prepare and start origin environment.
 */
def startOrigin(String username = "developer", String password = "developer") {
    downloadOcOrigin()
    timeout(time: 15, unit: 'MINUTES') {
        status = sh(
                script: "oc cluster up --public-hostname=\$(hostname -I | awk '{print \$1}') --base-dir ${ORIGIN_BASE_DIR} --enable=*,service-catalog,web-console --insecure-skip-tls-verify=true",
                returnStatus: true
        )
    }
    def url = sh(script: "echo \"\$(hostname -I | awk '{print \$1}')\"", returnStdout: true).trim()
    createUserOrigin("${username}", "https://${url}:8443")
    login("https://${url}:8443", "${username}", "${password}")
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


def buildStrimziImages() {
    sh "make docker_build"
    sh "make docker_tag"
}

def runSystemTests(String workspace, String testCases, String testProfile) {
    withMaven(mavenOpts: '-Djansi.force=true') {
        sh "mvn -f ${workspace}/systemtest/pom.xml -P all verify -Dgroups=${testProfile} -Dit.test=${testCases} -Djava.net.preferIPv4Stack=true -DtrimStackTrace=false -Dstyle.color=always --no-transfer-progress"
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