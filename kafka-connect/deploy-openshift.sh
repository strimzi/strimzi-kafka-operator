#!/bin/bash

# Based on the EnMasse deploy script
# https://github.com/EnMasseProject/enmasse/blob/master/templates/install/deploy-openshift.sh

function runcmd() {
    local cmd=$1
    local description=$2

    if [ "$GUIDE" == "true" ]; then
        echo "$description:"
        echo ""
        echo "...."
        echo "$cmd"
        echo "...."
        echo ""
    else
        bash -c "$cmd"
    fi
}

function docmd() {
    local cmd=$1
    if [ -z $GUIDE ] || [ "$GUIDE" == "false" ]; then
        $cmd
    fi
}

if which oc &> /dev/null
then :
else
    echo "Cannot find oc command, please check path to ensure it is installed"
    exit 1
fi

SCRIPTDIR=`dirname $0`
OPENSHIFT_TEMPLATE=$SCRIPTDIR/resources/openshift-template.yaml

DEFAULT_USER=developer
DEFAULT_NAMESPACE=myproject
TEMPLATE_NAME=barnabas-connect
TEMPLATE_PARAMS=""

while getopts hm:n:p:u:v opt; do
    case $opt in
        m)
            MASTER_URI=$OPTARG
            ;;
        n)
            NAMESPACE=$OPTARG
            ;;
        p)
            TEMPLATE_PARAMS="$OPTARG $TEMPLATE_PARAMS"
            ;;
        u)
            OS_USER=$OPTARG
            USER_REQUESTED=true
            ;;
        v)
            set -x
            ;;
        h)
            echo "usage: deploy-openshift.sh [options]"
            echo
            echo "deploy the Kafka Connect suite into a running OpenShift cluster"
            echo
            echo "optional arguments:"
            echo "  -h                   show this help message"
            echo "  -m MASTER            OpenShift master URI to login against (default: https://localhost:8443)"
            echo "  -n NAMESPACE         OpenShift project name to install Kafka Connect into (default: $DEFAULT_NAMESPACE)"
            echo "  -p PARAMS            Custom template parameters to pass to Kafka Connect template ('cat $SCRIPTDIR/resources/openshift-template.yaml' to get a list of available parameters)"
            echo "  -u USER              OpenShift user to run commands as (default: $DEFAULT_USER)"
            echo
            exit
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit
            ;;
    esac
done

if [ -z "$OS_USER" ]
then
    OS_USER=$DEFAULT_USER
fi

if [ -z "$NAMESPACE" ]
then
    NAMESPACE=$DEFAULT_NAMESPACE
fi

runcmd "oc login -u $OS_USER $OC_ARGS $MASTER_URI" "Login as $OS_USER"

AVAILABLE_PROJECTS=`docmd "oc projects -q"`

for proj in $AVAILABLE_PROJECTS
do
    if [ "$proj" == "$NAMESPACE" ]; then
        runcmd "oc project $proj" "Select project"
        break
    fi
done

CURRENT_PROJECT=`docmd "oc project -q"`
if [ "$CURRENT_PROJECT" != "$NAMESPACE" ]; then
    runcmd "oc new-project $NAMESPACE" "Create new project $NAMESPACE"
fi

runcmd "oc create sa kafka-connect -n $NAMESPACE" "Create service account for address controller"
runcmd "oc policy add-role-to-user view system:serviceaccount:${NAMESPACE}:kafka-connect" "Add permissions for viewing OpenShift resources to default user"

runcmd "oc process -f $OPENSHIFT_TEMPLATE $TEMPLATE_PARAMS | oc create -n $NAMESPACE -f -" "Instantiate Kafka Connect template"