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

package org.sparklinedata.druid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRowWithSchema
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.joda.time.Interval
import org.sparklinedata.druid.client.{DruidQueryServerClient, QueryResultRow, SelectResultRow}
import org.sparklinedata.druid.metadata.{DruidMetadataCache, DruidRelationInfo, DruidSegmentInfo, HistoricalServerAssignment}

abstract class DruidPartition extends Partition {
  def queryClient : DruidQueryServerClient
  def intervals : List[Interval]
  def segIntervals : List[(DruidSegmentInfo, Interval)]

  def setIntervalsOnQuerySpec(q : QuerySpec) : QuerySpec = {
    if ( segIntervals == null) {
      q.setIntervals(intervals)
    } else {
      q.setSegIntervals(segIntervals)
    }
  }

  def setIntervalsOnSelectSpec(q : SelectSpec) : SelectSpec = {
    if ( segIntervals == null) {
      q.setIntervals(intervals)
    } else {
      q.setSegIntervals(segIntervals)
    }
  }
}

class HistoricalPartition(idx: Int, val hs : HistoricalServerAssignment) extends DruidPartition {
  override def index: Int = idx
  def queryClient : DruidQueryServerClient = new DruidQueryServerClient(hs.server.host)

  def intervals : List[Interval] = hs.segmentIntervals.map(_._2)

  def segIntervals : List[(DruidSegmentInfo, Interval)] = hs.segmentIntervals
}

class BrokerPartition(idx: Int,
                      val broker : String,
                      val i : Interval) extends DruidPartition {
  override def index: Int = idx
  def queryClient : DruidQueryServerClient = new DruidQueryServerClient(broker)
  def intervals : List[Interval] = List(i)

  def segIntervals : List[(DruidSegmentInfo, Interval)] = null
}

abstract class AbstarctDruidRDD(sqlContext: SQLContext,
               val drInfo : DruidRelationInfo,
               val dQuery : DruidQuerySpec)
  extends RDD[InternalRow](sqlContext.sparkContext, Nil) {

  override protected def getPartitions: Array[Partition] = {
    if (dQuery.queryHistoricalServer) {
    val hAssigns = DruidMetadataCache.assignHistoricalServers(
      drInfo.host,
      drInfo.druidDS.name,
      drInfo.options,
      dQuery.intervalSplits
    )
      var idx = -1
      val numSegmentsPerQuery = drInfo.options.numSegmentsPerHistoricalQuery

      (for(
        hA <- hAssigns;
           segIns <- hA.segmentIntervals.sliding(numSegmentsPerQuery,numSegmentsPerQuery)
      ) yield {
        idx = idx + 1
        new HistoricalPartition(idx, new HistoricalServerAssignment(hA.server, segIns))
      }
        ).toArray
  } else {
      val broker = DruidMetadataCache.getDruidClusterInfo(drInfo.host,
        drInfo.options).curatorConnection.getBroker
      dQuery.intervalSplits.zipWithIndex.map(t => new BrokerPartition(t._2, broker, t._1)).toArray
    }
  }

  /**
    * conversion from Druid values to Spark values. Most of the conversion cases are handled by
    * cast expressions in the [[org.apache.spark.sql.execution.Project]] operator above the
    * DruidRelation Operator; but Strings need to be converted to [[UTF8String]] strings.
    *
    * @param f
    * @param druidVal
    * @return
    */
  def sparkValue(f : StructField, druidVal : Any) : Any = f.dataType match {
    case TimestampType if druidVal.isInstanceOf[Double] =>
      druidVal.asInstanceOf[Double].longValue().asInstanceOf[SQLTimestamp]
    case StringType if druidVal != null => UTF8String.fromString(druidVal.toString)
    case LongType if druidVal.isInstanceOf[BigInt] =>
      druidVal.asInstanceOf[BigInt].longValue()
    case LongType if druidVal.isInstanceOf[Double] =>
      druidVal.asInstanceOf[Double].longValue()
    case _ => druidVal
  }
}


class DruidRDD(sqlContext: SQLContext,
               drInfo : DruidRelationInfo,
               dQuery : DruidQuery)  extends
  AbstarctDruidRDD(sqlContext, drInfo, dQuery) {

  def druidQuery = this.dQuery

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val p = split.asInstanceOf[DruidPartition]
    val mQry =  p.setIntervalsOnQuerySpec(dQuery.q) // dQuery.q.setIntervals(p.intervals)
    val client = p.queryClient
    Utils.logQuery(client, mQry)
    val dr = client.executeQueryAsStream(mQry)
    context.addTaskCompletionListener{ context => dr.closeIfNeeded() }
    val r = new InterruptibleIterator[QueryResultRow](context, dr)
    val schema = dQuery.schema(drInfo)
    r.map { r =>
      // log.info(s"Received  druid row from ${client.host}: $r")
      new GenericInternalRowWithSchema(schema.fields.map(f => sparkValue(f, r.event(f.name))),
        schema)
    }
  }
}

class DruidSelectRDD(sqlContext: SQLContext,
                     drInfo : DruidRelationInfo,
                     dQuery : DruidSelectQuery)
  extends AbstarctDruidRDD(sqlContext, drInfo, dQuery) {

  def druidEventColumnName(spkColNm : String) : String = {
    if ( spkColNm == drInfo.timeDimensionCol ) {
      "timestamp"
    } else {
      drInfo.sourceToDruidMapping(spkColNm).name
    }
  }

  override def sparkValue(f : StructField, iVal : Any) : Any = {
    val druidVal : String = if (iVal == null) null else iVal.toString

    f.dataType match {

      case x if x == null => null
      case TimestampType =>
        DateTimeUtils.stringToTimestamp(UTF8String.fromString(druidVal)).getOrElse(null)
      case StringType => UTF8String.fromString(druidVal.toString)
      case BooleanType => druidVal.toBoolean
      case DateType =>
        DateTimeUtils.stringToDate(UTF8String.fromString(druidVal)).getOrElse(null)
      case DoubleType => druidVal.toDouble
      case FloatType => druidVal.toFloat
      case decTyp: DecimalType => Decimal(druidVal)
      case IntegerType => druidVal.toInt
      case ByteType => druidVal.toByte
      case LongType => druidVal.toLong
      case ShortType => druidVal.toShort
      case _ => druidVal
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val p = split.asInstanceOf[DruidPartition]
    val mQry = p.setIntervalsOnSelectSpec(dQuery.q)
    Utils.logQuery(mQry)
    val client = p.queryClient
    val dr = client.executeSelectAsStreamWithResult(mQry)
    context.addTaskCompletionListener{ context => dr.closeIfNeeded() }
    val r = new InterruptibleIterator[SelectResultRow](context, dr)
    val schema = dQuery.schema
    r.map { r =>
      new GenericInternalRowWithSchema(schema.fields.map{f =>
        sparkValue(f, r.event(druidEventColumnName(f.name)))
      },
        schema)
    }
  }

}