package com.myCompany.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object ReadS3Data {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ReadS3Data").setMaster("local")
    val sc = new SparkContext(conf)

    // to read data from S3 bucket we need to add below dependency
    //in case of Maven:
   //  <dependency>
    //      <groupId>org.apache.hadoop</groupId>
     //     <artifactId>hadoop-aws</artifactId>
      //    <version>2.9.1</version>
     // </dependency>
    
    // Read each line of my book into an RDD
    val p = sc.textFile("s3n://Accesskey:Secretkey@bucketname/filename")


    print(p.count())
  }

}

