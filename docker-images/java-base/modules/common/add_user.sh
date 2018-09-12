#!/bin/sh

set -e

# Create a user and group used to launch processes
# We use the ID 185 for the group as well as for the user.
# This ID is registered static ID which makes it safe to use.
groupadd -r $USER -g 185 && useradd -u 185 -r -g $USER -m -d $HOME -s /sbin/nologin -c "user" $USER 
chown -R $USER:root $HOME
chmod -R 775 $HOME
