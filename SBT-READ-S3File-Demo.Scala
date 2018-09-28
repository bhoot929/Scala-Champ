#this code is using SBT dependencies and read data from S3. later it will print the word count.

package com.swapnil


import org.apache.spark._



/** Count up how many of each word appears in a book as simply as possible. */
object WordCount {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    //Logger.getLogger("org").setLevel(Level.ERROR)
    //val conf = new SparkConf().setAppName("WordCount").setMaster("local")
   // val sc = new SparkContext()
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local", "WordCount")
    // Read each line of my book into an RDD
    val input = sc.textFile("s3a://partition929/book.txt")

    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))

    // Count up the occurrences of each word
    val wordCounts = words.countByValue()

    // Print the results.
    wordCounts.foreach(println)
  }

}


#content of SBT file: built.sbt

name := "Scala_SBT_test"

version := "0.1"

scalaVersion := "2.11.8"

organization := "com.swapnil"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.1"
)


finally to run code on EMR cluster use below code:
spark-submit --class com.swapnil.WordCount scala_sbt_test_2.11-0.1.jar

Where class is name of your class and jar should be present on /home/hadoop

