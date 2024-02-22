package com.privasapien.assignment

import com.privasapien.assignment.constant.AssignmentConstants.{awsAccessKey, awsSecretAccessKey, metaDataOutputPath, s3Bucket, sampleDataOutputPath}
import com.privasapien.assignment.processors.IntelligentSamplingExtractor.getIntelligentSamples
import com.privasapien.assignment.processors.MetaDataExtractor.extractMetaData
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Driver {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder
      .config("spark.hadoop.fs.s3a.access.key", awsAccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", awsSecretAccessKey)
      .appName("ETLJob")
      .master("local")
      .getOrCreate()

    /***
     * // Following spark props can be fetched from Airflow Dag at run time,
     *
     * val inputBucket = spark.conf.get("spark.privasapien.assignment.input.bucket")
     * val awsAccessKey = spark.conf.get("spark.privasapien.assignment.aws.accessKey")
     * val awsSecretAccessKey = spark.conf.get("spark.privasapien.assignment.aws.secretAccessKey")
     * val metaDataOutputPath = spark.conf.get("spark.privasapien.assignment.metadataOutput.path")
     * val sampleWithMaxFillRate = spark.conf.get("spark.privasapien.assignment.sampleOutput.path")
     */


    val metaData: DataFrame = extractMetaData(s3Bucket)(spark)
    metaData.write.mode(SaveMode.Overwrite).parquet(metaDataOutputPath)
    val samplesWithMaximumDataFillRate = getIntelligentSamples(metaDataOutputPath)
    samplesWithMaximumDataFillRate.write.parquet(sampleDataOutputPath)

    spark.stop()
  }

}
