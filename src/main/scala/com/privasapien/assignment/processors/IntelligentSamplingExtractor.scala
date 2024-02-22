package com.privasapien.assignment.processors

import org.apache.spark.sql.functions.{col, desc, lit, monotonically_increasing_id, trim, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object IntelligentSamplingExtractor {

  def getIntelligentSamples(metaDataDFPath: String)(implicit spark: SparkSession): DataFrame = {

    val metaDataDF = spark.read.parquet(metaDataDFPath)
    val sampledDF = stratifiedSampling(metaDataDF, k = 10)
    val prioritizedDF = prioritizeBasedOnFillRate(sampledDF)
    prioritizedDF
  }

  private def stratifiedSampling(metadataDF: DataFrame, k: Int)(implicit spark: SparkSession): DataFrame = {
    val fileFormats = metadataDF.select("FileFormat").distinct().collect().map(_.getString(0))
    val stratifiedSamples = fileFormats.map { format =>
      val formatDF = metadataDF.filter(s"FileFormat === '$format'")
      val randomStartIndex = scala.util.Random.nextInt(k)
      val sampledDF = formatDF.withColumn("row_number", monotonically_increasing_id() % k)
        .filter(s"row_number === $randomStartIndex")
        .drop("row_number")
      val sampledWithFillRate = sampledDF.withColumn("FillRate", udf(estimateFillRate _).apply(col("Path"), lit(format)))
      sampledWithFillRate
    }
    stratifiedSamples.reduce(_ union _)
  }

  private def prioritizeBasedOnFillRate(sampledDF: DataFrame): DataFrame = {
    sampledDF.orderBy(desc("FillRate"))
  }

  private def estimateFillRate(filePath: String, fileFormat: String)(implicit spark: SparkSession): Double = {
    val sampleDF = spark.read.format(fileFormat).option("header", "true").option("inferSchema", "true").load(filePath)
    val completenessExpr = (
      (sampleDF.columns.map(c => when(functions.length(trim(col(c).cast("string"))) =!= 0, 1).otherwise(0)).reduce(_ + _) / sampleDF.columns.length.toDouble) * 100
      ).alias("completeness")
    val overallCompleteness = sampleDF.select(completenessExpr).head().getAs[Double]("completeness")
    overallCompleteness
  }

}
