/*
 * Copyright 2015 TouchType Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.redshift

import java.net.URI
import java.sql.{Connection, Date, SQLException, Timestamp}

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.util.control.NonFatal

import com.databricks.spark.redshift.Parameters.MergedParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types._

/**
 * Functions to write data to Redshift.
 *
 * At a high level, writing data back to Redshift involves the following steps:
 *
 *   - Use the spark-avro library to save the DataFrame to S3 using Avro serialization. Prior to
 *     saving the data, certain data type conversions are applied in order to work around
 *     limitations in Avro's data type support and Redshift's case-insensitive identifier handling.
 *
 *     While writing the Avro files, we use accumulators to keep track of which partitions were
 *     non-empty. After the write operation completes, we use this to construct a list of non-empty
 *     Avro partition files.
 *
 *   - If there is data to be written (i.e. not all partitions were empty), then use the list of
 *     non-empty Avro files to construct a JSON manifest file to tell Redshift to load those files.
 *     This manifest is written to S3 alongside the Avro files themselves. We need to use an
 *     explicit manifest, as opposed to simply passing the name of the directory containing the
 *     Avro files, in order to work around a bug related to parsing of empty Avro files (see #96).
 *
 *   - Start a new JDBC transaction and disable auto-commit. Depending on the SaveMode, issue
 *     DELETE TABLE or CREATE TABLE commands, then use the COPY command to instruct Redshift to load
 *     the Avro data into the appropriate table.
 */
private[redshift] class RedshiftWriter(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client) {

  private val log = LoggerFactory.getLogger(getClass)

  // Visible for testing.
  private[redshift] def createTableSql(data: DataFrame,
                                       params: MergedParameters,
                                       table: TableName,
                                       ifNotExists: Boolean): String = {
    val schemaSql = jdbcWrapper.schemaString(data.schema)

    val primaryKeyDef = params.primaryKey
      .map(primaryKey => s", PRIMARY KEY($primaryKey)")
      .getOrElse("")

    val distStyleDef = params.distStyle match {
      case Some(style) => s"DISTSTYLE $style"
      case None => ""
    }
    val distKeyDef = params.distKey match {
      case Some(key) => s"DISTKEY ($key)"
      case None => ""
    }
    val sortKeyDef = params.sortKeySpec.getOrElse("")

    val ifNotExistsDef = if (ifNotExists) " IF NOT EXISTS" else ""

    s"CREATE TABLE$ifNotExistsDef $table ($schemaSql$primaryKeyDef) $distKeyDef $sortKeyDef"
  }

  private[redshift] def dropTableSql(table: TableName): String = {
    s"DROP TABLE IF EXISTS $table"
  }

  private[redshift] def deleteExistingSql(params: MergedParameters): String = {
    val table = params.table.get
    val stagingTable = params.stagingTable.get
    val whereClause = params.primaryKey.get
      .split(",")
      .map(key => s"$table.$key = $stagingTable.$key").mkString(" AND ")

    s"DELETE from $table USING $stagingTable WHERE $whereClause"
  }

  private[redshift] def insertSql(params: MergedParameters): String = {
    val table = params.table.get
    val stagingTable = params.stagingTable.get
    val selectAllStaging = s"(SELECT * FROM $stagingTable)"

    s"INSERT INTO $table $selectAllStaging"
  }

  private[redshift] def copySql(sqlContext: SQLContext,
                      params: MergedParameters,
                      table: TableName,
                      creds: AWSCredentialsProvider,
                      manifestUrl: String): String = {
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)
    val fixedUrl = Utils.fixS3Url(manifestUrl)
    val format = params.tempFormat match {
      case "AVRO" => "AVRO 'auto'"
      case csv if csv == "CSV" || csv == "CSV GZIP" => csv + s" NULL AS '${params.nullString}'"
    }
    s"COPY $table FROM '$fixedUrl' CREDENTIALS '$credsString' FORMAT AS " +
      s"$format manifest ${params.extraCopyOptions}"
  }

  /**
    * Generate COMMENT SQL statements for the table and columns.
    */
  private[redshift] def commentActions(tableComment: Option[String], schema: StructType):
      List[String] = {
    tableComment.toList.map(desc => s"COMMENT ON TABLE %s IS '${desc.replace("'", "''")}'") ++
    schema.fields
      .withFilter(f => f.metadata.contains("description"))
      .map(f => s"""COMMENT ON COLUMN %s."${f.name.replace("\"", "\\\"")}""""
              + s" IS '${f.metadata.getString("description").replace("'", "''")}'")
  }

  private def executePreActions(conn: Connection,
                                params: MergedParameters,
                                data: DataFrame): Unit = {
    val preActions = commentActions(params.description, data.schema) ++ params.preActions
    preActions.foreach { action =>
      val actionSql = if (action.contains("%s")) action.format(params.table.get) else action
      log.info(s"Executing preAction: $actionSql")
      executeSqlStatement(conn, actionSql)
    }
  }

  private def executePostActions(conn: Connection,
                                 params: MergedParameters): Unit = {
    params.postActions.foreach { action =>
      val actionSql = if (action.contains("%s")) action.format(params.table.get) else action
      log.info("Executing postAction: " + actionSql)
      executeSqlStatement(conn, actionSql)}
  }

  private def executeSqlStatement(conn: Connection, sql: String): Unit = {
    jdbcWrapper.executeInterruptibly(conn.prepareStatement(sql))
  }

  private def logSqlStatement(msg: String, statement: String): Unit = {
    log.info(s"$msg\n \t> $statement")
  }

  /**
    * Overwrite the data in a prod redshift table with new data by:
    * 1) Dropping the existing prod table;
    * 2) Creating a new prod table
    * 3) Using a LOAD command to copy the temp data in s3 into the new prod table.
    */
  private def doRedshiftOverwrite(conn: Connection,
                                  data: DataFrame,
                                  params: MergedParameters,
                                  creds: AWSCredentialsProvider,
                                  manifestUrlOpt: Option[String]): Unit = {
    executePreActions(conn, params, data)

    // Drop the existing prod table.
    val dropProdStatement = dropTableSql(params.table.get)
    logSqlStatement(s"Dropping existing prod table ${params.table.get}", dropProdStatement)
    executeSqlStatement(conn, dropProdStatement)

    // Create a new prod table.
    val createProdStatement = createTableSql(data, params, params.table.get, ifNotExists = false)
    logSqlStatement(s"Creating prod table ${params.table.get}", createProdStatement)
    executeSqlStatement(conn, createProdStatement)

    manifestUrlOpt.foreach { manifestUrl =>
      try {
        // Copy data from s3 to the new prod table.
        val copyToProdStatement = copySql(data.sqlContext,
                                          params,
                                          params.table.get,
                                          creds,
                                          manifestUrl)
        logSqlStatement(s"Copying data from s3 to prod table ${params.table.get}",
                        copyToProdStatement)
        executeSqlStatement(conn, copyToProdStatement)
      } catch {
        case e: SQLException =>
          handleLoadException(conn, e, params)
      }
    }

    executePostActions(conn, params)
  }

  /**
   * Load new data into a prod redshift table by:
   * 1) Creating a staging table in redshift.
   * 2) Using a LOAD command to copy the temp data in s3 into the staging table.
   * 3) Deleting rows in the prod table that are in the staging table.
   * 4) Inserting all rows from the staging table into the prod table.
   * 5) Deleting the staging table
   */
  private def doRedshiftLoad(conn: Connection,
                             data: DataFrame,
                             params: MergedParameters,
                             creds: AWSCredentialsProvider,
                             manifestUrl: Option[String]): Unit = {
    executePreActions(conn, params, data)

    // Create the staging redshift table. Drop previously existing staging table first.
    val dropStagingStatement = dropTableSql(params.stagingTable.get)
    logSqlStatement(s"Dropping existing staging table ${params.stagingTable.get}",
                    dropStagingStatement)
    executeSqlStatement(conn, dropStagingStatement)

    val createStagingStatement = createTableSql(data,
                                                params,
                                                params.stagingTable.get,
                                                ifNotExists = false)
    logSqlStatement(s"Creating staging table ${params.stagingTable.get}", createStagingStatement)
    executeSqlStatement(conn, createStagingStatement)

    // Create the prod redshift table if it doesn't exist already.
    val createProdStatement = createTableSql(data, params, params.table.get, ifNotExists = true)
    logSqlStatement(s"Creating table ${params.table.get}", createProdStatement)
    executeSqlStatement(conn, createProdStatement)

    manifestUrl.foreach { manifestUrl =>
      try {
        // Copy from s3 to the staging table.
        val copyToStagingStatement = copySql(data.sqlContext,
                                             params,
                                             params.stagingTable.get,
                                             creds,
                                             manifestUrl)
        logSqlStatement(s"Copying data to ${params.stagingTable.get}", copyToStagingStatement)
        executeSqlStatement(conn, copyToStagingStatement)

        // Upsert rows from the staging table into the prod table.
        val deleteExistingRowsStatement = deleteExistingSql(params)
        logSqlStatement(s"Deleting existing rows in ${params.table.get}",
                        deleteExistingRowsStatement)
        executeSqlStatement(conn, deleteExistingRowsStatement)

        val insertNewRowsStatement = insertSql(params)
        logSqlStatement(s"Inserting new rows into ${params.table.get}", insertNewRowsStatement)
        executeSqlStatement(conn, insertNewRowsStatement)

        // Drop staging table.
        val dropStagingStatement = dropTableSql(params.stagingTable.get)
        logSqlStatement(s"Dropping table ${params.stagingTable.get}", dropStagingStatement)
        executeSqlStatement(conn, dropStagingStatement)

      } catch {
        case e: SQLException =>
          handleLoadException(conn, e, params)
      }
    }

    executePostActions(conn, params)
  }

  private def handleLoadException(conn: Connection,
                                  e: Exception,
                                  params: MergedParameters): Unit = {
    conn.rollback()

    // Try to query Redshift's STL_LOAD_ERRORS table to figure out why the load failed.
    // See http://docs.aws.amazon.com/redshift/latest/dg/r_STL_LOAD_ERRORS.html for details.
    log.error("SQLException thrown while running COPY query; will attempt to retrieve " +
              "more information by querying the STL_LOAD_ERRORS table", e)
    val errorLookupQuery =
      """
        | SELECT *
        | FROM stl_load_errors
        | WHERE query = pg_last_query_id()
      """.stripMargin
    val detailedException: Option[SQLException] = try {
      val results =
        jdbcWrapper.executeQueryInterruptibly(conn.prepareStatement(errorLookupQuery))
      if (results.next()) {
        val errCode = results.getInt("err_code")
        val errReason = results.getString("err_reason").trim
        val columnLength: String =
          Option(results.getString("col_length"))
          .map(_.trim)
          .filter(_.nonEmpty)
          .map(n => s"($n)")
          .getOrElse("")
        val exceptionMessage =
          s"""
             |Error (code $errCode) while loading data into Redshift: "$errReason"
             |Table name: ${params.table.get}
             |Column name: ${results.getString("colname").trim}
             |Column type: ${results.getString("type").trim}$columnLength
             |Raw line: ${results.getString("raw_line")}
             |Raw field value: ${results.getString("raw_field_value")}
                  """.stripMargin
        Some(new SQLException(exceptionMessage, e))
      } else {
        None
      }
    } catch {
      case NonFatal(e2) =>
        log.error("Error occurred while querying STL_LOAD_ERRORS", e2)
        None
    }
    throw detailedException.getOrElse(e)
  }

  /**
   * Serialize temporary data to S3, ready for Redshift COPY, and create a manifest file which can
   * be used to instruct Redshift to load the non-empty temporary data partitions.
   *
   * @return the URL of the manifest file in S3, in `s3://path/to/file/manifest.json` format, if
   *         at least one record was written, and None otherwise.
   */
  private def unloadData(
      sqlContext: SQLContext,
      data: DataFrame,
      tempDir: String,
      tempFormat: String,
      nullString: String): Option[String] = {
    // spark-avro does not support Date types. In addition, it converts Timestamps into longs
    // (milliseconds since the Unix epoch). Redshift is capable of loading timestamps in
    // 'epochmillisecs' format but there's no equivalent format for dates. To work around this, we
    // choose to write out both dates and timestamps as strings.
    // For additional background and discussion, see #39.

    // Convert the rows so that timestamps and dates become formatted strings.
    // Formatters are not thread-safe, and thus these functions are not thread-safe.
    // However, each task gets its own deserialized copy, making this safe.
    val conversionFunctions: Array[Any => Any] = data.schema.fields.map { field =>
      field.dataType match {
        case DateType =>
          val dateFormat = Conversions.createRedshiftDateFormat()
          (v: Any) => {
            if (v == null) null else dateFormat.format(v.asInstanceOf[Date])
          }
        case TimestampType =>
          val timestampFormat = Conversions.createRedshiftTimestampFormat()
          (v: Any) => {
            if (v == null) null else timestampFormat.format(v.asInstanceOf[Timestamp])
          }
        case _ => (v: Any) => v
      }
    }

    // Use Spark accumulators to determine which partitions were non-empty.
    val nonEmptyPartitions =
      sqlContext.sparkContext.accumulableCollection(mutable.HashSet.empty[Int])

    val convertedRows: RDD[Row] = data.rdd.mapPartitions { iter: Iterator[Row] =>
      if (iter.hasNext) {
        nonEmptyPartitions += TaskContext.get.partitionId()
      }
      iter.map { row =>
        val convertedValues: Array[Any] = new Array(conversionFunctions.length)
        var i = 0
        while (i < conversionFunctions.length) {
          convertedValues(i) = conversionFunctions(i)(row(i))
          i += 1
        }
        Row.fromSeq(convertedValues)
      }
    }

    // Convert all column names to lowercase, which is necessary for Redshift to be able to load
    // those columns (see #51).
    val schemaWithLowercaseColumnNames: StructType =
      StructType(data.schema.map(f => f.copy(name = f.name.toLowerCase)))

    if (schemaWithLowercaseColumnNames.map(_.name).toSet.size != data.schema.size) {
      throw new IllegalArgumentException(
        "Cannot save table to Redshift because two or more column names would be identical" +
        " after conversion to lowercase: " + data.schema.map(_.name).mkString(", "))
    }

    // Update the schema so that Avro writes date and timestamp columns as formatted timestamp
    // strings. This is necessary for Redshift to be able to load these columns (see #39).
    val convertedSchema: StructType = StructType(
      schemaWithLowercaseColumnNames.map {
        case StructField(name, DateType, nullable, meta) =>
          StructField(name, StringType, nullable, meta)
        case StructField(name, TimestampType, nullable, meta) =>
          StructField(name, StringType, nullable, meta)
        case other => other
      }
    )

    val writer = sqlContext.createDataFrame(convertedRows, convertedSchema).write
    (tempFormat match {
      case "AVRO" =>
        writer.format("com.databricks.spark.avro")
      case "CSV" =>
        writer.format("com.databricks.spark.csv")
          .option("escape", "\"")
          .option("nullValue", nullString)
      case "CSV GZIP" =>
        writer.format("csv")
          .option("escape", "\"")
          .option("nullValue", nullString)
          .option("compression", "gzip")
    }).save(tempDir)

    if (nonEmptyPartitions.value.isEmpty) {
      None
    } else {
      // See https://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html
      // for a description of the manifest file format. The URLs in this manifest must be absolute
      // and complete.

      // The partition filenames are of the form part-r-XXXXX-UUID.fileExtension.
      val fs = FileSystem.get(URI.create(tempDir), sqlContext.sparkContext.hadoopConfiguration)
      val partitionIdRegex = "^part-(?:r-)?(\\d+)[^\\d+].*$".r
      val filesToLoad: Seq[String] = {
        val nonEmptyPartitionIds = nonEmptyPartitions.value.toSet
        fs.listStatus(new Path(tempDir)).map(_.getPath.getName).collect {
          case file @ partitionIdRegex(id) if nonEmptyPartitionIds.contains(id.toInt) => file
        }
      }
      // It's possible that tempDir contains AWS access keys. We shouldn't save those credentials to
      // S3, so let's first sanitize `tempdir` and make sure that it uses the s3:// scheme:
      val sanitizedTempDir = Utils.fixS3Url(
        Utils.removeCredentialsFromURI(URI.create(tempDir)).toString).stripSuffix("/")
      val manifestEntries = filesToLoad.map { file =>
        s"""{"url":"$sanitizedTempDir/$file", "mandatory":true}"""
      }
      val manifest = s"""{"entries": [${manifestEntries.mkString(",\n")}]}"""
      val manifestPath = sanitizedTempDir + "/manifest.json"
      val fsDataOut = fs.create(new Path(manifestPath))
      try {
        fsDataOut.write(manifest.getBytes("utf-8"))
      } finally {
        fsDataOut.close()
      }
      Some(manifestPath)
    }
  }

  /**
   * Write a DataFrame to a Redshift table, using S3 and Avro or CSV serialization
   */
  def saveToRedshift(
      sqlContext: SQLContext,
      data: DataFrame,
      saveMode: SaveMode,
      params: MergedParameters) : Unit = {
    if (params.table.isEmpty) {
      throw new IllegalArgumentException(
        "For save operations you must specify a Redshift table name with the 'dbtable' parameter")
    }

    val creds: AWSCredentialsProvider =
      AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)

    for (
      redshiftRegion <- Utils.getRegionForRedshiftCluster(params.jdbcUrl);
      s3Region <- Utils.getRegionForS3Bucket(params.rootTempDir, s3ClientFactory(creds))
    ) {
     val regionIsSetInExtraCopyOptions =
       params.extraCopyOptions.contains(s3Region) && params.extraCopyOptions.contains("region")
     if (redshiftRegion != s3Region && !regionIsSetInExtraCopyOptions) {
       log.error("The Redshift cluster and S3 bucket are in different regions " +
         s"($redshiftRegion and $s3Region, respectively). In order to perform this cross-region " +
         s"""write, you must add "region '$s3Region'" to the extracopyoptions parameter. """ +
         "For more details on cross-region usage, see the README.")
     }
    }

    // When using the Avro tempformat, log an informative error message in case any column names
    // are unsupported by Avro's schema validation:
    if (params.tempFormat == "AVRO") {
      for (fieldName <- data.schema.fieldNames) {
        // The following logic is based on Avro's Schema.validateName() method:
        val firstChar = fieldName.charAt(0)
        val isValid = (firstChar.isLetter || firstChar == '_') && fieldName.tail.forall { c =>
          c.isLetterOrDigit || c == '_'
        }
        if (!isValid) {
          throw new IllegalArgumentException(
            s"The field name '$fieldName' is not supported when using the Avro tempformat. " +
              "Try using the CSV tempformat  instead. For more details, see " +
              "https://github.com/databricks/spark-redshift/issues/84")
        }
      }
    }

    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)

    //Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))

    // Save the table's rows to S3:
    val manifestUrl = unloadData(
      sqlContext,
      data,
      tempDir = params.createPerQueryTempDir(),
      tempFormat = params.tempFormat,
      nullString = params.nullString)
    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
    conn.setAutoCommit(false)
    try {
      val table: TableName = params.table.get
      if (saveMode == SaveMode.Overwrite) {
        log.info(s"Overwriting data in $table")
        doRedshiftOverwrite(conn, data, params, creds, manifestUrl)
      } else {
        log.info(s"Loading new data into $table")
        doRedshiftLoad(conn, data, params, creds, manifestUrl)
      }
      conn.commit()

    } catch {
      case NonFatal(e) =>
        try {
          log.error("Exception thrown during Redshift load; will roll back transaction", e)
          conn.rollback()
        } catch {
          case NonFatal(e2) =>
            log.error("Exception while rolling back transaction", e2)
        }
        throw e
    } finally {
      conn.close()
    }
  }
}

object DefaultRedshiftWriter extends RedshiftWriter(
  DefaultJDBCWrapper,
  awsCredentials => new AmazonS3Client(awsCredentials))
