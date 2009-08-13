#! /bin/bash
#
#   ::::::::> Clobi JOB AGENT <::::::::
#   Job Agent initialization shellscript
#
#   Contact: http://gehrcke.de
#
#   Copyright (C) 2009 Jan-Philip Gehrcke
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#########################################################################

echo -e "***********************************************************"
echo -e "            Clobi (by Jan-Philip Gehrcke)"
echo -e "           Job Agent initialization script"
echo -e "    /root/clobi/jobagent_init.sh invoked from rc.local"
echo -e "***********************************************************\n\n"

echo "# setting up environment variables..."
JobAgentDir=$(pwd)
echo JobAgentDir: ${JobAgentDir}
UserDataFile=${JobAgentDir}/userdata
echo UserDataFile: ${UserDataFile}
InstanceIdFile=${JobAgentDir}/instanceid
echo InstanceIdFile: ${InstanceIdFile}

echo "# detect location of metadataserver..."
if [ -e /var/nimbus-metadata-server-url ]; then
    MetaDataServerURL=$(cat /var/nimbus-metadata-server-url)
    is_ec2=false
else
    echo "  /var/nimbus-metadata-server-url not found -> Assume EC2."
    MetaDataServerURL=http://169.254.169.254/
    is_ec2=true
fi

# EC2's 2008-08-08 path doesn't work anymore. ugly. But Nimbus also
# supports 2007-01-19 and 2007-03-01.
echo "# receive user-data..."
wget ${MetaDataServerURL}/2007-03-01/user-data -O ${UserDataFile}
if [ $? -ne 0 ]; then
    echo "  error while retrieving userdata: jobagent_init exit."
    exit $?
fi

# Nimbus' metadata server currently does not support instance ID.
if ${is_ec2}; then
    echo "# EC2: receive instance ID..."
    wget ${MetaDataServerURL}/2008-08-08/meta-data/instance-id -O ${InstanceIdFile}
    if [ $? -ne 0 ]; then
        echo "  error while retrieving instance ID: jobagent_init exit."
        exit $?
    fi
    InstanceID=$(cat ${InstanceIdFile})
    echo "   Instance ID: ${InstanceID}"
fi

# this is just for convenience during testing: no VM rebuild everytime I want
# to test a new job agent :-)
wget http://www.physik.uni-wuerzburg.de/~jgehrcke/jobagent.tar.bz2 -O jobagent.tar.bz2
tar xjf jobagent.tar.bz2

echo "# running jobagent.py --userdatafile ${UserDataFile}"
/opt/bin/python2.6 jobagent.py --userdatafile ${UserDataFile}
