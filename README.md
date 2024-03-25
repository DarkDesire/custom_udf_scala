# Custom UDF Scala Project for Spark SQL

## Overview

This project provides a collection of custom User-Defined Functions (UDFs) for Spark SQL, aimed at enhancing data processing capabilities within Spark applications. The UDFs cover a range of functionalities, from manipulating numerical sequences to performing operations on strings and more. This project is designed to work with Databricks Runtime 14.3 LTS, leveraging Scala 2.12.15.

## Databricks Runtime 14.3 LTS

- **Operating System**: Ubuntu 22.04.3 LTS
- **Java Version**: Zulu 8.74.0.17-CA-linux64
- **Scala Version**: 2.12.15
- **Python Version**: 3.10.12
- **R Version**: 4.3.1
- **Delta Lake Version**: 3.1.0
- **Apache Spark Version**: 3.5.0

For more details, refer to the [Databricks documentation](https://docs.databricks.com/en/release-notes/runtime/14.3lts.html).

## Build Environment

- **SBT**: 1.9.8
- **JDK**: Java 1.8.0_402 @ Azul Zulu: 8.76.0.17, Windows
- **Scala Version**: 2.12.15

## Project Structure

The project consists of the following Scala files and a build configuration:

- `build.sbt`: SBT build configuration file.
- `MyCustomArrayUDFs.scala`: Contains UDFs for operations on arrays, such as removing consecutive NaN values.
- `MyCustomUDFs.scala`: Provides UDFs for generating random numbers, incrementing numbers, calculating the combined length of two strings, and filtering based on a numerical condition.

## Building and Deployment
To build the project, ensure you have SBT installed and run the following command in the project's root directory:

```bash
sbt package
```
This will compile the Scala code and package it into a JAR file, which can then be deployed to a Spark environment that matches the project's runtime dependencies.

## Custom UDFs
### Array Operations
* removeConsecutiveNans: Removes consecutive NaN values from a sequence of doubles, preserving the first occurrence in each series.
## Numerical and String Operations
* myRandom: Generates a random number. This function is marked as non-deterministic.
* plusOne: Increments an integer value by one.
* twoArgLen: Calculates the combined length of two strings.
* oneArgFilter: Filters entries where the input number is greater than 5.

## Usage
To use these UDFs in your Spark SQL applications, first register them with your SparkSession using the __registerAllUDFs__ method provided in each object. Once registered, the UDFs can be invoked directly in your Spark SQL queries.
