
def setupKubernetes() {
    // set SElinux to permisive mode
    sh(script: "sudo setenforce 0 || true")
    // Install conntrack
    sh(script: "sudo yum install -y conntrack")
    sh(script: "${workspace}/.jenkins/scripts/setup-kubernetes-s390x.sh")
}

def setupShellheck() {
    // setup shellcheck
    sh(script: "${workspace}/.azure/scripts/setup_shellcheck.sh")
}

def installHelm(String workspace) {
    sh(script: "${workspace}/.azure/scripts/setup-helm.sh")
}

def installYq(String workspace) {
    sh(script: "${workspace}/.jenkins/scripts/install_yq-s390x.sh")
}

def buildKeycloakAndOpa(String workspace) {
    sh(script: "${workspace}/.jenkins/scripts/build_keycloak_opa-s390x.sh")
}

def applys390xpatch(String workspace) {
    sh(script: "${workspace}/.jenkins/scripts/apply_s390x_patch.sh")
}

def buildStrimziImages() {
    sh(script: """
        eval \$(minikube docker-env)
        make MVN_ARGS='-DskipTests' all
        #MVN_ARGS='-Dsurefire.rerunFailingTestsCount=5 -Dfailsafe.rerunFailingTestsCount=2' make all
    """)
}

def runSystemTests(String workspace, String testCases, String testProfile, String testGroups, String excludeGroups, String testsInParallel) {
    def groupsTag = testGroups.isEmpty() ? "" : "-Dgroups=${testGroups} "
    def testcasesTag = testCases.isEmpty() ? "" : "-Dit.test=${testCases} "
    //withMaven(maven: 'maven-3.8.4', mavenOpts: '-Djansi.force=true') {
        sh(script: "mvn -f ${workspace}/systemtest/pom.xml verify " +
            "-P${testProfile} " +
            "${groupsTag}" +
            "-DexcludedGroups=${excludeGroups} " +
            "${testcasesTag}" +
            "-Dmaven.javadoc.skip=true -B -V " +
            "-Djava.net.preferIPv4Stack=true " +
            "-DtrimStackTrace=false " +
            "-Djansi.force=true " +
            "-Dstyle.color=always " +
            "--no-transfer-progress " +
            "-Dfailsafe.rerunFailingTestsCount=2 " +
            "-Djunit.jupiter.execution.parallel.enabled=false " +
            // sequence mode with testInParallel=1 otherwise parallel
            "-Djunit.jupiter.execution.parallel.config.fixed.parallelism=${testsInParallel}")
    //}
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
}

def sendMail(String address, String status, String prID, String prAuthor, String prTitle, String prUrl, String buildUrl) {
    //mail to:"${address}", subject:"Build of Strimzi PR#${prID} by ${prAuthor} - '${prTitle}' has ${status}", body:"PR link: ${prUrl}\nBuild link: ${buildUrl}"
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
