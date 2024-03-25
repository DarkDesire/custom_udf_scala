package com.example.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
 * Defines and registers custom User-Defined Functions (UDFs) for use in Spark SQL.
 * 
 * This object includes UDFs for generating random numbers, incrementing numbers, 
 * calculating the combined length of two strings, and filtering based on a numerical condition.
 */
object MyCustomUDFs {
  
  /**
   * Name of the UDF for generating a random number. This UDF does not take any arguments.
   * Note: This UDF is marked as non-deterministic, meaning it does not produce the same result for the same input.
   */
  val MY_RANDOM = "myRandom"
  
  /** UDF function for generating a random number. */
  val random = udf(() => Math.random()).asNondeterministic()

  /**
   * Name of the UDF for incrementing a number by one.
   */
  val PLUS_ONE_UDF = "plusOne"
  
  /** UDF function for adding one to the input value. */
  val plusOne = udf((x: Int) => x + 1)

  /**
   * Name of the UDF for calculating the combined length of two strings.
   */
  val TWO_ARG_LEN_UDF = "twoArgLen"
  
  /** UDF function for calculating the total length of two string inputs. */
  val twoArgLen = udf((_: String).length + (_: String).length())

  /**
   * Name of the UDF used for filtering entries where the input number is greater than 5.
   */
  val ONE_ARG_FILTER_UDF = "oneArgFilter"
  
  /** UDF function for filtering based on whether the input integer is greater than 5. */
  val oneArgFilter = udf((n: Int) => { n > 5 })
  
  /**
   * Registers all available custom UDFs with the provided SparkSession.
   * 
   * The method registers four UDFs: `myRandom`, `plusOne`, `twoArgLen`, and `oneArgFilter`,
   * each designed for different data manipulation tasks within Spark SQL queries.
   * 
   * @param spark the SparkSession with which to register the UDFs
   */
  def registerAllUDFs(spark: SparkSession) {
    spark.udf.register(MY_RANDOM, random)
    spark.udf.register(PLUS_ONE_UDF, plusOne)
    spark.udf.register(TWO_ARG_LEN_UDF, twoArgLen)
    spark.udf.register(ONE_ARG_FILTER_UDF, oneArgFilter)
  }
}
