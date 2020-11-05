#!/bin/bash

usage() {
  echo "Usage: $0 [USERNAME] [PASSWORD] [ARGUMENTS] ..."
  echo
  echo "$0 is a tool for obtaining an access token or a refresh token for the user or the client."
  echo
  echo " USERNAME    The username for user authentication"
  echo " PASSWORD    The password for user authentication (prompted for if not specified)"
  echo
  echo " If USERNAME and PASSWORD are not specified, client credentials as specified by --client-id and --secret will be used for authentication."
  echo
  echo " ARGUMENTS:"
  echo "   --quiet, -q                      No informational outputs"
  echo "   --insecure                       Allow http:// in token endpoint url"
  echo "   --access                         Return access_token rather than refresh_token"
  echo "   --endpoint  TOKEN_ENDPOINT_URL   Authorization server token endpoint"
  echo "   --client-id CLIENT_ID            Client id for client authentication - must be configured on authorization server"
  echo "   --secret    CLIENT_SECRET        Secret to authenticate the client"
  echo "   --scopes    SCOPES               Space separated list of scopes to request - default value: offline_access"
}


CLAIM=refresh_token
GRANT_TYPE=password
DEFAULT_SCOPES=offline_access

while [ $# -gt 0 ]
do
    case "$1" in
      "-q" | "--quiet")
          QUIET=1
          ;;
      --endpoint)
          shift
          TOKEN_ENDPOINT="$1"
          ;;
      --insecure)
          INSECURE=1
          ;;
      --access)
          CLAIM=access_token
          DEFAULT_SCOPES=""
          ;;
      --client-id)
          shift
          CLIENT_ID="$1"
          ;;
      --secret)
          shift
          CLIENT_SECRET="$1"
          ;;
      --scopes)
          shift
          SCOPES="$1"
          ;;
      --help)
          usage
          exit 1
          ;;
      *)
          if [ "$UNAME" == "" ]; then
            UNAME="$1"
          elif [ "$PASS" == "" ]; then
            PASS="$1"
          else
            >&2 echo "Unexpected argument!"
            exit 1
          fi
          ;;
    esac
    shift
done

if [ "$TOKEN_ENDPOINT" == "" ]; then
    >&2 echo "TOKEN_ENDPOINT not set. Use --endpoint with endpoint url to set it."
    exit 1
fi

if [ "$UNAME" != "" ] && [ "$PASS" == "" ]; then
    >&2 read -s -p "Password: " PASS
    >&2 echo
fi

if [ "$UNAME" == "" ] && [ "$CLIENT_ID" == "" ]; then
    echo "USERNAME not specified. Use --client-id and --secret to authenticate with client credentials."
    exit 1
fi

if [ "$CLIENT_ID" == "" ]; then
    [ "$QUIET" == "" ] && >&2 echo "CLIENT_ID not set. Using default value: kafka-cli"
    CLIENT_ID=kafka-cli
fi

if [ "$UNAME" == "" ]; then
    GRANT_TYPE=client_credentials
else
    USER_PASS_CLIENT="&username=${UNAME}&password=${PASS}&client_id=${CLIENT_ID}"
fi

if [ "$SCOPES" == "" ] && [ DEFAULT_SCOPES != "" ]; then
    [ "$QUIET" == "" ] && >&2 echo "SCOPES not set. Using default value: ${DEFAULT_SCOPES}"
    SCOPES="${DEFAULT_SCOPES}"
fi

if [ "$CLIENT_SECRET" != "" ]; then
    AUTH_VALUE=$(echo -n "$CLIENT_ID:$CLIENT_SECRET" | base64)
    AUTHORIZATION="Authorization: Basic $AUTH_VALUE"
fi

cmd=()
cmd+=(curl)
cmd+=(-s)
cmd+=(-X)
cmd+=(POST)
cmd+=($TOKEN_ENDPOINT)
if [[ "$AUTHORIZATION" != "" ]]; then cmd+=(-H); cmd+=("$AUTHORIZATION"); fi
cmd+=(-H)
cmd+=("Content-Type: application/x-www-form-urlencoded")
cmd+=(-d)
if [[ "$SCOPES" != "" ]]; then
  cmd+=("grant_type=${GRANT_TYPE}${USER_PASS_CLIENT}&scope=${SCOPES}")
else
  cmd+=("grant_type=${GRANT_TYPE}${USER_PASS_CLIENT}")
fi

[ "$QUIET" == "" ] && >&2 echo $(printf "'%s' " "${cmd[@]}")

result=`"${cmd[@]}"`

if [[ $(echo $result | grep $CLAIM) == "" ]]; then echo "Error: $result"; exit 1; fi

echo $result | awk -F "$CLAIM\":\"" '{printf $2}' | awk -F "\"" '{printf $1}'
