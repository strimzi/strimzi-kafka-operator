
def setupKubernetes() {
    // set SElinux to permisive mode
    sh(script: "sudo setenforce 0")
    // Install conntrack
    sh(script: "sudo yum install -y conntrack")
    sh(script: "${workspace}/.azure/scripts/setup-kubernetes.sh")
}

def clearImages() {
    sh(script: "docker rmi -f \$(docker images -q) 2>/dev/null || echo 'No more images to remove.'")
}

def installHelm(String workspace) {
    sh(script: "${workspace}/.azure/scripts/setup-helm.sh")
}

def installYq(String workspace) {
    sh(script: "${workspace}/.azure/scripts/install_yq.sh")
}

def buildStrimziImages() {
    sh(script: """
        eval \$(minikube docker-env)
        MVN_ARGS='-Dsurefire.rerunFailingTestsCount=5 -Dfailsafe.rerunFailingTestsCount=2' make docker_build
        make docker_tag
        make docker_push
    """)
}

def runSystemTests(String workspace, String testCases, String testProfile, String excludeGroups) {
    withMaven(mavenOpts: '-Djansi.force=true') {
        sh(script: "mvn -f ${workspace}/systemtest/pom.xml -P all verify " +
                "-Dgroups=${testProfile} " +
                "-DexcludedGroups=${excludeGroups} " +
                "-Dit.test=${testCases} " +
                "-Djava.net.preferIPv4Stack=true " +
                "-DtrimStackTrace=false " +
                "-Dstyle.color=always " +
                "--no-transfer-progress " +
                "-Dfailsafe.rerunFailingTestsCount=2")
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
