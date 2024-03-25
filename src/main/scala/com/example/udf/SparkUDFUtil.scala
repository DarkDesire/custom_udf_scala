package com.example.udf

import org.apache.spark.sql.SparkSession
import java.io.File
import java.net.{URI, URISyntaxException}

object SparkUDFUtil {
  def getSparkUDFModuleJarFilePath(): String = {
    // jar file path
    val uri =
      SparkUDFUtil.getClass.getProtectionDomain.getCodeSource.getLocation.toURI

    if (!isSparkUDFModuleLoadedAsJarFile(uri)) {
      throw new IllegalStateException(
        "Please load custom_udf_scala as jar file on classpath instead of as .class file on classpath"
      )
    }
    new File(uri).getPath
  }

  private def isSparkUDFModuleLoadedAsJarFile(uri: URI): Boolean =
    uri.toString.endsWith(".jar")

  def registerAllUDFs(spark: SparkSession) {
    MyCustomUDFs.registerAllUDFs(spark)
    MyCustomArrayUDFs.registerAllUDFs(spark)
  }
}
