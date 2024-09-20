from pyspark.sql import SparkSession
import pyspark.sql.functions as F

BOOTSTRAP_SERVERS = "confluent-local-broker-1:55748"
TOPIC = "server_logs"
DB_HOST = 'aws-0-us-west-1.pooler.supabase.com'
DB_NAME = 'postgres'
DB_PORT = '6543'
DB_USERNAME = 'postgres.yjajdrzerpwleueoclby'
DB_PASSWORD = 'HelloSQL2221!' 
DB_URL = f'jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?prepareThreshold=0'
DB_PROPERTIES = {'user': DB_USERNAME, 'password': DB_PASSWORD, 'driver': 'org.postgresql.Driver'}


def output_to_console(df, output_mode='append'):
    df.writeStream.outputMode(output_mode).format('console').options(truncate=False).start()


def write_to_postgres(df, table_name: str, write_mode='append', output_mode: str = 'append'):
    write = lambda df, epoch_id: df.write.jdbc(url=DB_URL, table=table_name, mode=write_mode, properties=DB_PROPERTIES)
    return df.writeStream.foreachBatch(write).outputMode(output_mode).start()


def main():
    spark = SparkSession.builder.appName('StructuredStreamingKafka').getOrCreate()
    kafka_stream_df = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS)
        .option('subscribe', TOPIC)
        .load()
    )

    df = kafka_stream_df.select(kafka_stream_df.value.cast('string'))
    df = df.select(F.regexp_replace(F.col('value'), r'[\[\]"]', '').alias('value'))
    df = df.select(F.split(df.value, ' ').alias('data'))
    df = df.select(
        F.col('data').getItem(0).alias('ip_address'),
        F.col('data').getItem(1).alias('user_name'),
        F.col('data').getItem(2).cast('integer').alias('user_id'),
        F.col('data').getItem(3).cast('timestamp').alias('timestamp'),
        F.col('data').getItem(4).alias('http_method'),
        F.col('data').getItem(5).alias('path'),
        F.col('data').getItem(6).cast('integer').alias('status_code')
    )
    write_to_postgres(df, 'server_logs')

    errors_df = df.filter(F.col("status_code").isin(404, 500)).groupBy("path").agg(
            F.count(F.when(F.col("status_code") == 404, 1)).alias("404_errors"),
            F.count(F.when(F.col("status_code") == 500, 1)).alias("500_errors"),
            F.count("*").alias("total_errors")
    )
    write_to_postgres(errors_df, 'errors_by_path', write_mode='overwrite', output_mode='complete')

    window_duration = '1 minute'  # define window duration
    window_df = (
        df.groupBy(F.window('timestamp', window_duration), 'ip_address')
        .agg(F.count('ip_address').alias('count'))
        .select(
            F.col('window.start').alias('window_start'),
            F.col('window.end').alias('window_end'),
            'ip_address',
            'count'
        ).withColumn('dos_attack', F.col('count') > 100)  # add dos attack col
        .filter(F.col('dos_attack') == True)  # filter to include only dos attacks
        .orderBy(F.col('window_start').desc())  # sort by window time in desc order
    )
    output_to_console(window_df, output_mode='complete')
    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
