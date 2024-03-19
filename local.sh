SPARK_COMMAND='spark-submit
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,io.delta:delta-core_2.12/2.3.0,com.amazonaws:aws-java-sdk-s3:1.12.401,com.amazonaws:aws-java-sdk-secretsmanager:1.12.401
--class MapperEntrypoint
--files examples/confs/log4j2.properties
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp -Dlog4j.configuration=log4j2.properties"
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j2.properties"
--deploy-mode client target/scala-2.12/metabolic-core-assembly-SNAPSHOT.jar
--dp.crawl false
--dp.region "eu-central-1"
--dp.iamrole "random"
--dp.envPrefix "local/local"
--dp.database "local"
--dp.environment production
--dp.checkpointLocation examples/metadata
--dp.mysqldb factorial
--dp.stream true
--dp.historical true
--configFile'

eval $SPARK_COMMAND examples/confs/model.conf