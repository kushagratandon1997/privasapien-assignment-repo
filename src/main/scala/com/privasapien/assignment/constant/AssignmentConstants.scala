package com.privasapien.assignment.constant

object AssignmentConstants {

  val awsAccessKey = "AKIAW3MEDIWJXRWDA6MP"
  val awsSecretAccessKey = "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"

  // aws configs
  val s3Bucket = "interview-s3-bucket-privasapien"
  val metaDataOutputPath = s"s3://$s3Bucket/metadata-output"
  val sampleDataOutputPath = s"s3://$s3Bucket/sample-data-output"
}
