#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# NOTE: this script runs inside hudi-ci-bundle-validation container
# $WORKDIR/jars/ is supposed to be mounted to a host directory where bundle jars are placed
# TODO: $JAR_COMBINATIONS should have different orders for different jars to detect class loading issues

WORKDIR=/opt/bundle-validation
HIVE_DATA=${WORKDIR}/data/hive
JAR_DATA=${WORKDIR}/data/jars
UTILITIES_DATA=${WORKDIR}/data/utilities

run_hive_sync () {
    echo "::warning::validate.sh setting up hive sync"
    #put config files in correct place
    cp $HIVE_DATA/spark-defaults.conf $SPARK_HOME/conf/
    cp $HIVE_DATA/hive-site.xml $HIVE_HOME/conf/
    ln -sf $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml
    cp $DERBY_HOME/lib/derbyclient.jar $SPARK_HOME/jars/

    $DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
    $HIVE_HOME/bin/hiveserver2 &
    echo "::warning::validate.sh hive setup complete. Testing"
    $SPARK_HOME/bin/spark-shell --jars $JAR_DATA/spark.jar < $HIVE_DATA/validate.scala
    if [ "$?" -ne 0 ]; then
        echo "::error::validate.sh failed hive testing)"
        exit 1
    fi
    echo "::warning::validate.sh hive testing succesfull. Cleaning up hive sync"
    #remove config files
    rm -f $SPARK_HOME/jars/derbyclient.jar
    unlink $SPARK_HOME/conf/hive-site.xml
    rm -f $HIVE_HOME/conf/hive-site.xml
    rm -f $SPARK_HOME/conf/spark-defaults.conf
}

test_utilities_bundle () {
    echo "::warning::validate.sh running deltastreamer"
    $SPARK_HOME/bin/spark-submit --driver-memory 8g --executor-memory 8g \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
    $JARS_BEGIN $JARS_ARG \
    --props $UTILITIES_DATA/parquet-dfs-compact.props \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --source-class org.apache.hudi.utilities.sources.ParquetDFSSource \
    --source-ordering-field date_col --table-type MERGE_ON_READ \
    --target-base-path file://${OUTPUT_DIR} \
    --target-table ny_hudi_tbl  --op UPSERT
    echo "::warning::validate.sh done with deltastreamer"

    OUTPUT_SIZE=$(du -s ${OUTPUT_DIR} | awk '{print $1}')
    if [[ -z $OUTPUT_SIZE || "$OUTPUT_SIZE" -lt "1000" ]]; then
        echo "::error::validate.sh deltastreamer output folder is much smaller than expected)" 
        exit 1
    fi

    echo "::warning::validate.sh validating deltastreamer in spark shell"
    SHELL_COMMAND="$SPARK_HOME/bin/spark-shell --jars $JARS_ARG $SHELL_ARGS -i $COMMANDS_FILE"
    LOGFILE="$WORKDIR/submit.log"
    $SHELL_COMMAND >> $LOGFILE 
    if [ "$?" -ne 0 ]; then
        SHELL_RESULT=$(cat $LOGFILE | grep "Counts don't match")
        echo "::error::validate.sh $SHELL_RESULT)"
        exit 1
    fi
    echo "::warning::validate.sh done validating deltastreamer in spark shell"
}


# run_hive_sync
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi

SHELL_ARGS=$(cat $UTILITIES_DATA/shell_args)

echo "::warning::validate.sh testing utilities bundle"
JARS_BEGIN=""
JARS_ARG=$JAR_DATA/utilities.jar
OUTPUT_DIR=/tmp/hudi-deltastreamer-ny/
COMMANDS_FILE=$UTILITIES_DATA/commands.scala
test_utilities_bundle
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done testing utilities bundle"

echo "::warning::validate.sh testing utilities slim bundle"
JARS_BEGIN="--jars"
JARS_ARG="$JAR_DATA/spark.jar $JAR_DATA/utilities-slim.jar"
OUTPUT_DIR=/tmp/hudi-deltastreamer-ny-slim/
COMMANDS_FILE=$UTILITIES_DATA/slimcommands.scala
test_utilities_bundle
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done testing utilities slim bundle"

