#!/bin/sh
set -eu
cd /app
# picks up JAVA_TOOL_OPTIONS from docker-compose (e.g. -Xms/-Xmx)
exec java ${JAVA_TOOL_OPTIONS:-} -jar app.jar
