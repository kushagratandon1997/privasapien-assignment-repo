package com.privasapien.assignment.processors

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MetaDataExtractor {

  def extractMetaData(inputS3Bucket: String)(implicit spark: SparkSession): DataFrame = {

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(hadoopConf)
    val allFiles: Array[FileStatus] = fs.listStatus(new Path(inputS3Bucket))
    def recursivelyFetchMetadata(files: Array[FileStatus]): Seq[(String, Long, Long, String)] = {
      files.flatMap { fileStatus =>
        if (fileStatus.isDirectory) {
          val subFiles = fs.listStatus(fileStatus.getPath)
          recursivelyFetchMetadata(subFiles)
        } else {
          Seq(fetchMetadata(fileStatus))
        }
      }
    }

    import spark.implicits._

    val metadataSeq = recursivelyFetchMetadata(allFiles)
    val metadataDF: DataFrame = metadataSeq.toDF("Path", "Size", "ModificationTime", "FileFormat")
    metadataDF

  }

  private def getFileFormat(filePath: String): String = {
    val extension = filePath.split('.').lastOption.getOrElse("")
    extension.toLowerCase
  }


  private def fetchMetadata(fileStatus: FileStatus): (String, Long, Long, String) = {
    val filePath = fileStatus.getPath.toString
    val fileSize = fileStatus.getLen
    val fileModificationTime = fileStatus.getModificationTime
    val fileFormat = getFileFormat(filePath)
    (filePath, fileSize, fileModificationTime, fileFormat)
  }

}
