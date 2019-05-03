/**
 * Function for setup the test cluster.
 */
def setupEnvironment(String workspace, String openshift) {
    sh "rm -rf ~/.kube"
    sh "mkdir -p /tmp/openshift"
    clearImages()

    status = sh(
        script: "wget ${openshift} -O openshift.tar.gz",
        returnStatus: true
    )
    //////////////////////////////////////////////////

    sh "tar xzf openshift.tar.gz -C /tmp/openshift --strip-components 1"
    sh "sudo cp /tmp/openshift/oc /usr/bin/oc"
    sh "sudo rm -rf /tmp/openshift/"
    sh "sudo rm -rf openshift.tar.gz"

    timeout(time: 10, unit: 'MINUTES') {
        status = sh(
            script: "oc cluster up --base-dir ${workspace}/origin/ --enable=*,service-catalog,web-console --insecure-skip-tls-verify=true",
            returnStatus: true
        )
    }

    timeout(time: 15, unit: 'MINUTES') {
        if (status != 0) {
            sleep(10)
            sh "oc cluster down"
            sh "oc cluster up --base-dir ${workspace}/origin/ --enable=*,service-catalog,web-console --insecure-skip-tls-verify=true"
        }
    }

    sh "export KUBECONFIG=${workspace}/origin/kube-apiserver/admin.kubeconfig"
    def KUBECONFIG="${workspace}/origin/kube-apiserver/admin.kubeconfig"
    sh "oc login -u system:admin"
    sh "oc --config ${KUBECONFIG} adm policy add-cluster-role-to-user cluster-admin developer"

    sh "oc label node localhost rack-key=zone"
    sh "oc apply -f https://gist.githubusercontent.com/scholzj/614065a081ad92669c32f45894510c8c/raw/96d1a6539a99f0dce2d5eb02a8f15e6eb109a9d6/strimzi-admin.yaml"
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


def buildStrimzi() {
    sh "mvn clean install -DskipTests"
    sh "make docker_build"
    sh "make docker_tag"
}

def runSystemTests(String workspace, String testCases, String testProfile) {
    sh "mvn -f ${workspace}/systemtest/pom.xml -P ${testProfile} verify -Dit.test=${testCases} -Djava.net.preferIPv4Stack=true -DtrimStackTrace=false"
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

return this