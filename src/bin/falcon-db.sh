#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

. ${BASEDIR}/bin/falcon-config.sh 'server' falcon

if test -z ${JAVA_HOME}
then
    JAVA_BIN=java
else
    JAVA_BIN=${JAVA_HOME}/bin/java
fi

while [[ ${1} =~ ^\-D ]]; do
  JAVA_PROPERTIES="${JAVA_PROPERTIES} ${1}"
  shift
done

# set the client class path
FALCONCPPATH="$FALCONCPPATH:${BASEDIR}/client/lib/*"

${JAVA_BIN} ${JAVA_PROPERTIES} -cp ${FALCONCPPATH} -Dfalcon.log.dir=$HOME org.apache.falcon.tools.FalconStateStoreDBCLI "${@}"
