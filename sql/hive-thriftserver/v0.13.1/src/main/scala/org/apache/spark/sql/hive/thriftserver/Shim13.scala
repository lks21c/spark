/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.sql.{Date, Timestamp}
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map => SMap}
import scala.math._

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.Logging
import org.apache.spark.sql.execution.SetCommand
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}
import org.apache.spark.sql.{Row => SparkRow, SQLConf, SchemaRDD}

/**
 * A compatibility layer for interacting with Hive version 0.13.1.
 */
private[thriftserver] object HiveThriftServerShim {
  val version = "0.13.1"

  def setServerUserName(
      sparkServiceUGI: UserGroupInformation,
      sparkCliService:SparkSQLCLIService) = {
    setSuperField(sparkCliService, "serviceUGI", sparkServiceUGI)
  }
}

private[hive] class SparkSQLDriver(val _context: HiveContext = SparkSQLEnv.hiveContext)
  extends AbstractSparkSQLDriver(_context) {
  override def getResults(res: JList[_]): Boolean = {
    if (hiveResponse == null) {
      false
    } else {
      res.asInstanceOf[JArrayList[String]].addAll(hiveResponse)
      hiveResponse = null
      true
    }
  }
}

private[hive] class SparkExecuteStatementOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)(
    hiveContext: HiveContext,
    sessionToActivePool: SMap[SessionHandle, String])
  // NOTE: `runInBackground` is set to `false` intentionally to disable asynchronous execution
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, false) with Logging {

  private var result: SchemaRDD = _
  private var iter: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    logDebug("CLOSING")
  }

  def addNonNullColumnValue(from: SparkRow, to: ArrayBuffer[Any],  ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to += from.getString(ordinal)
      case IntegerType =>
        to += from.getInt(ordinal)
      case BooleanType =>
        to += from.getBoolean(ordinal)
      case DoubleType =>
        to += from.getDouble(ordinal)
      case FloatType =>
        to += from.getFloat(ordinal)
      case DecimalType() =>
        to += from.getAs[BigDecimal](ordinal).bigDecimal
      case LongType =>
        to += from.getLong(ordinal)
      case ByteType =>
        to += from.getByte(ordinal)
      case ShortType =>
        to += from.getShort(ordinal)
      case DateType =>
        to += from.getAs[Date](ordinal)
      case TimestampType =>
        to +=  from.getAs[Timestamp](ordinal)
      case BinaryType | _: ArrayType | _: StructType | _: MapType =>
        val hiveString = HiveContext.toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to += hiveString
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion)
    if (!iter.hasNext) {
      resultRowSet
    } else {
      // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
      val maxRows = maxRowsL.toInt
      var curRow = 0
      while (curRow < maxRows && iter.hasNext) {
        val sparkRow = iter.next()
        val row = ArrayBuffer[Any]()
        var curCol = 0
        while (curCol < sparkRow.length) {
          if (sparkRow.isNullAt(curCol)) {
            row += null
          } else {
            addNonNullColumnValue(sparkRow, row, curCol)
          }
          curCol += 1
        }
        resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
        curRow += 1
      }
      resultRowSet
    }
  }

  def getResultSetSchema: TableSchema = {
    logInfo(s"Result Schema: ${result.queryExecution.analyzed.output}")
    if (result.queryExecution.analyzed.output.size == 0) {
      new TableSchema(new FieldSchema("Result", "string", "") :: Nil)
    } else {
      val schema = result.queryExecution.analyzed.output.map { attr =>
        new FieldSchema(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
      }
      new TableSchema(schema)
    }
  }

  def run(): Unit = {
    logInfo(s"Running query '$statement'")
    setState(OperationState.RUNNING)
    try {
      result = hiveContext.sql(statement)
      logDebug(result.queryExecution.toString())
      result.queryExecution.logical match {
        case SetCommand(Some((SQLConf.THRIFTSERVER_POOL, Some(value))), _) =>
          sessionToActivePool(parentSession.getSessionHandle) = value
          logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
        case _ =>
      }

      val groupId = round(random * 1000000).toString
      hiveContext.sparkContext.setJobGroup(groupId, statement)
      sessionToActivePool.get(parentSession.getSessionHandle).foreach { pool =>
        hiveContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
      }
      iter = {
        val useIncrementalCollect =
          hiveContext.getConf("spark.sql.thriftServer.incrementalCollect", "false").toBoolean
        if (useIncrementalCollect) {
          result.toLocalIterator
        } else {
          result.collect().iterator
        }
      }
      dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
      setHasResultSet(true)
    } catch {
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        setState(OperationState.ERROR)
        logError("Error executing query:", e)
        throw new HiveSQLException(e.toString)
    }
    setState(OperationState.FINISHED)
  }
}
