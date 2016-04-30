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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.sources.druid.{DataSourceFilterTransform, DruidPlanner}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.joda.time.Interval
import org.sparklinedata.druid.metadata.{DruidDataType, DruidRelationInfo}

case class DruidOperatorAttribute(exprId : ExprId, name : String, dataType : DataType)

trait  DruidQuerySpec {
  def logQuery : Unit
  def schema(dInfo : DruidRelationInfo) : StructType
  def queryHistoricalServer : Boolean
  def intervalSplits : List[Interval]
}

/**
 *
 * @param q
 * @param intervalSplits
 * @param outputAttrSpec attributes to be output from the PhysicalRDD. Each output attribute is
 *                       based on an Attribute in the originalPlan. The association is based
 *                       on the ExprId.
 */
case class DruidQuery(q : QuerySpec,
                      queryHistoricalServer : Boolean,
                      intervalSplits : List[Interval],
                       outputAttrSpec :Option[List[DruidOperatorAttribute]]
                       ) extends DruidQuerySpec {

  def this(q : QuerySpec,
           queryHistoricalServer : Boolean = false) =
    this(q, queryHistoricalServer, q.intervals.map(Interval.parse(_)), None)

  def logQuery : Unit = Utils.logQuery(this)

  private def schemaFromQuerySpec(dInfo : DruidRelationInfo) : StructType = {

    val fields : List[StructField] = q.dimensions.map{d =>
      new StructField(d.outputName, d.sparkDataType(dInfo.druidDS))
    } ++
      q.aggregations.map {a =>
        new StructField(a.name, a.sparkDataType(dInfo.druidDS))
      } ++
      q.postAggregations.map{ ps =>
        ps.map {p =>
          new StructField(p.name, p.sparkDataType(dInfo.druidDS))
        }
      }.getOrElse(Nil)

    StructType(fields)
  }

  private def schemaFromOutputSpec : StructType = {
    val fields : List[StructField] = outputAttrSpec.get.map {
      case DruidOperatorAttribute(eId, nm, dT) => new StructField(nm, dT)
    }
    StructType(fields)
  }

  def schema(dInfo : DruidRelationInfo) : StructType =
    outputAttrSpec.map(o => schemaFromOutputSpec).getOrElse(schemaFromQuerySpec(dInfo))

  private def outputAttrsFromQuerySpec(dInfo : DruidRelationInfo) : Seq[Attribute] = {
    schemaFromQuerySpec(dInfo).fields.map { f =>
      AttributeReference(f.name, f.dataType)()
    }
  }

  private def outputAttrsFromOutputSpec : Seq[Attribute] = {
    outputAttrSpec.get.map {
      case DruidOperatorAttribute(eId, nm, dT) => AttributeReference(nm, dT)(eId)
    }
  }

  def outputAttrs(dInfo : DruidRelationInfo) : Seq[Attribute] =
    outputAttrSpec.map(o => outputAttrsFromOutputSpec).getOrElse(outputAttrsFromQuerySpec(dInfo))
}

case class DruidSelectQuery(q: SelectSpec,
                            queryHistoricalServer: Boolean,
                            intervalSplits: List[Interval],
                            schema: StructType
                           ) extends DruidQuerySpec {
  def logQuery : Unit = Utils.logQuery(this)
  def schema(dInfo : DruidRelationInfo) : StructType = schema
}

case class DruidRelation (val info : DruidRelationInfo,
                                       val dQuery : Option[DruidQuery])(
  @transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {
  override val needConversion: Boolean = false

  override def schema: StructType =
    dQuery.map(_.schema(info)).getOrElse(info.sourceDF(sqlContext).schema)

  def buildInternalScan : RDD[InternalRow] =
    dQuery.map(new DruidRDD(sqlContext, info, _)).getOrElse(
      info.sourceDF(sqlContext).queryExecution.toRdd
    )

  def buildScan(): RDD[Row] =
    buildInternalScan.asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    /*
    - if dQuery is set call buildScan
    - construct a DSBldr
    - transform by applying reqCOls and filters
    - if at the end we have a DSB then return a DruidSelectRDD
    - else buildScan
     */

    dQuery.map { dq =>
      buildScan()
    }.getOrElse {
      val selRDD = DataSourceFilterTransform(
        DruidSelectQueryBuilder(info),
        requiredColumns.filter(_ != info.timeDimensionCol),
        filters).map(
        selectQuery(requiredColumns, _)
      ).map(
        new DruidSelectRDD(sqlContext, info, _).asInstanceOf[RDD[Row]]
      )
      selRDD.getOrElse(buildScan())
    }

  }

  private def selectQuery(requiredColumns: Array[String],
                  dsb : DruidSelectQueryBuilder) : DruidSelectQuery = {
    val ss = SelectSpec(
      "select",
      dsb.drInfo.druidDS.name,
      dsb.dimensions,
      dsb.metrics,
      dsb.filterSpec,
      PagingSpec(Map(),
        DruidPlanner.getConfValue(sqlContext,
          DruidPlanner.DRUID_SELECT_QUERY_PAGESIZE)
      )
    )

    val dsSchema : StructType = sqlContext.table(info.sourceDFName).schema
    val fields = requiredColumns.map(c =>
      dsSchema(c).copy()
    )

    DruidSelectQuery(ss,
      dsb.drInfo.options.queryHistoricalServers,
      dsb.queryIntervals.get,
      StructType(fields)
    )
  }
}
