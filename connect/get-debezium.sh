#!/usr/bin/env sh
# Downloads the Debezium Postgres connector plugin into connect/plugins (used by docs/31).
set -e
VERSION=3.3.1.Final
cd "$(dirname "$0")/plugins"
curl -fsSL -C - -o debezium.tar.gz "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/$VERSION/debezium-connector-postgres-$VERSION-plugin.tar.gz"
tar -xzf debezium.tar.gz && rm debezium.tar.gz
echo "Debezium $VERSION installed in connect/plugins/debezium-connector-postgres"
