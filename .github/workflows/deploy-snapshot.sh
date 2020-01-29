#!/bin/bash
#
# Copyright 2017-2020 O2 Czech Republic, a.s.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


set -e

VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version)

echo ${MAVEN_SETTINGS} > ~/.m2/settings.xml
echo ${GOOGLE_CREDENTIALS} > /tmp/google-credentials.json

export GOOGLE_APPLICATION_CREDENTIALS=/tmp/google-credentials.json

if echo ${VERSION} | grep SNAPSHOT >/dev/null && echo ${GITHUB_REPOSITORY} | grep O2-Czech-Republic >/dev/null; then
  mvn deploy -Prelease-snapshot
fi

