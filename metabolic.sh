#!/bin/bash


# Set the path to your Spark installation
SPARK_HOME=/Users/margon/Code/Metabolic/spark-3.3.2-bin-hadoop3/bin/

# Set the path to the directory containing your application jar file
APP_DIR=s3a://factorial-metabolic-gitpod

# Set the name of your application jar file
APP_JAR=metabolic-core-assembly-SNAPSHOT.jar

# Set the name of the main class in your application
MAIN_CLASS=

# Initialize the configuration file variable
CONFIG_FILE=

# Set default values for Spark configuration properties
SPARK_CONF='--deploy-mode client
--master local[*] '

# SPARK_CONF='--deploy-mode cluster
# --master spark://metabolic-master:7077 '
# Set default values for Metabolic configuration properties
METABOLIC_CONF='
--dp.region eu-central-1
--dp.iamrole gitpod-main-MetabolicRole
--dp.crawl false
--dp.envPrefix "gitpod/local"
--dp.database "gitpod_local_ddbb"
--dp.environment gitpod
--dp.environment-simple gitpod
--dp.mappings_bucket  /metabolic_scripts
--dp.checkpointLocation . '

# Parse command-line arguments
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    run)
    MAIN_CLASS=MapperEntrypoint
    shift
    ;;
    check)
    MAIN_CLASS=MetabolicAppCheck
    shift
    ;;
    --full-refresh)
    HISTORICAL_FLAG="--dp.historical true"
    shift
    ;;
    --streaming)
    STREAMING_FLAG="--dp.stream true"
    shift
    ;;
    *)
    # Assume it's the config file
    CONFIG_FILE=$key
    shift
    ;;
esac
done

# Check if a main class has been specified
if [[ -z "$MAIN_CLASS" ]] || [[ -z "$CONFIG_FILE" ]]
then
    echo "Usage: $0 {run|check} [--full-refresh] [--streaming] <config_file>"
    exit 1
fi
echo

# Submit the Spark application to the local cluster
$SPARK_HOME/spark-submit  \
    --packages com.amazonaws:aws-java-sdk-s3:1.12.401,com.amazonaws:aws-java-sdk-secretsmanager:1.12.401,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2
    --class $MAIN_CLASS  \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
    $SPARK_CONF \
    $APP_DIR/$APP_JAR \
    ${HISTORICAL_FLAG:-"--dp.historical false"} ${STREAMING_FLAG:-"--dp.stream false"} $METABOLIC_CONF --configFile $CONFIG_FILE


echo
#eval $COMMAND