package com.example.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import scala.collection.Seq

/** Provides custom Array User-Defined Functions (UDFs) for Spark SQL.
  *
  * This object contains methods for registering UDFs that perform operations on
  * arrays, including the removal of consecutive `NaN` (Not a Number) values
  * from a sequence of doubles.
  */
object MyCustomArrayUDFs {

  /** Name of the UDF for removing consecutive `NaN` values from a sequence of
    * doubles.
    */
  val REMOVE_CONSECUTIVE_NANS = "removeConsecutiveNans"

  /** Registers all available custom UDFs with the provided SparkSession.
    *
    * @param spark
    *   the SparkSession with which to register the UDFs
    */
  def registerAllUDFs(spark: SparkSession) {
    spark.udf.register(REMOVE_CONSECUTIVE_NANS, removeConsecutiveNans _)
  }

  /** Removes consecutive `NaN` values from a given sequence of doubles.
    *
    * This method iterates through the provided sequence and removes any
    * consecutive occurrences of `NaN`, while preserving the first occurrence in
    * each consecutive series of `NaN` values.
    *
    * @param doubles
    *   a sequence of Double values, potentially containing `NaN` values
    * @return
    *   a sequence of Double values with consecutive `NaN`s removed
    */
  private def removeConsecutiveNans(doubles: Seq[Double]): Seq[Double] = {
    val withoutConsecutiveNans = doubles.foldRight(List[Double]()) {
      (current, acc) =>
        if (current.isNaN && acc.headOption.map(_.isNaN).getOrElse(false)) acc
        else current :: acc
    }
    withoutConsecutiveNans
  }
}
