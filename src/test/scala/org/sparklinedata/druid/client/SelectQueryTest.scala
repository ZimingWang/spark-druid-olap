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

package org.sparklinedata.druid.client

import com.github.nscala_time.time.Imports._
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.sources.druid.DruidPlanner
import org.scalatest.BeforeAndAfterAll
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

class SelectQueryTest extends BaseTest with BeforeAndAfterAll with Logging {

  val flatStarSchemaSelect =
    """
      |{
      |  "factTable" : "orderLineItemPartSupplier_select",
      |  "relations" : []
      | }
    """.stripMargin.replace('\n', ' ')

  override def beforeAll() = {
    super.beforeAll()

    sql(s"""CREATE TABLE if not exists orderLineItemPartSupplierBase_select(o_orderkey integer,
             o_custkey integer,
      o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string,
      o_clerk string,
      o_shippriority integer, o_comment string, l_partkey integer, l_suppkey integer,
      l_linenumber integer,
      l_quantity double, l_extendedprice double, l_discount double, l_tax double,
      l_returnflag string,
      l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
      l_shipinstruct string,
      l_shipmode string, l_comment string, order_year string, ps_partkey integer,
      ps_suppkey integer,
      ps_availqty integer, ps_supplycost double, ps_comment string, s_name string, s_address string,
      s_phone string, s_acctbal double, s_comment string, s_nation string,
      s_region string, p_name string,
      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string,
      p_retailprice double,
      p_comment string, c_name string , c_address string , c_phone string , c_acctbal double ,
      c_mktsegment string , c_comment string , c_nation string , c_region string)
      USING com.databricks.spark.csv
      OPTIONS (path "src/test/resources/tpch/datascale1/orderLineItemPartSupplierCustomer.small",
      header "false", delimiter "|")""".stripMargin)

    sql(
      s"""CREATE TABLE if not exists orderLineItemPartSupplier_select
      USING org.sparklinedata.druid
      OPTIONS (sourceDataframe "orderLineItemPartSupplierBase_select",
      timeDimensionColumn "l_shipdate",
      druidDatasource "tpch",
      druidHost "localhost",
      queryHistoricalServers "true",
          |nonAggregateQueryHandling "push_project_and_filters",
      columnMapping '$colMapping',
      functionalDependencies '$functionalDependencies',
      starSchema '$flatStarSchemaSelect')""".stripMargin
    )
  }

  test("noQuery") { td =>
    val df = sql("select * from orderLineItemPartSupplier_select")
    df.explain(true)
    df.show(10)
  }

  test("intervalFilter", {
    val shipDtPredicate = dateTime('l_shipdate) <= (dateTime("1997-12-01") - 90.day)
    date"""
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier_select
      where $shipDtPredicate and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
"""
  },
    1, true, true
  )

  test("sparkIntervalFilter1", {
    """
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier_select
      where l_shipdate >  cast( '1993-05-19 00:00:00' as timestamp)
             and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
    """
  },
    1, true, true
  )

  test("sparkIntervalFilter2", {
    """
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier_select
      where l_shipdate >  '1993-05-19 00:00:00'
             and ((s_nation = 'FRANCE' and c_nation = 'GERMANY') or
                                  (c_nation = 'FRANCE' and s_nation = 'GERMANY')
                                 )
"""
  },
    1, true, true
  )

  test("sparkIntervalFilter4", {
    """
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier_select
      where l_shipdate >  cast( '1993-05-19 00:00:00' as timestamp)
    """
  },
    1, true, true
  )

  /*
  TODO: druid returns data, but being filtered out in Spark's Filter Op.
   */
  test("sparkIntervalFilter3", {
    """
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier_select
      where l_shipdate >  cast( '1993-05-19 00:00:00' as timestamp)
            and l_shipdate <  '1993-05-20 00:10:01'
    """
  },
    1, true, true
  )

  test("join1", {
    """
       select q1.l_shipdate
       from (
         select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
         from orderLineItemPartSupplier_select
      where l_shipdate <  '1993-01-03 00:10:01'
      ) q1 join (
       select l_returnflag as f, l_linestatus as s, l_shipdate, s_region, s_nation, c_nation
               from orderLineItemPartSupplier_select
            where l_shipdate <  '1993-01-03 00:10:01'
      ) q2 on q1.l_shipdate = q2.l_shipdate
    """.stripMargin
  },
    2, true, true
  )


}

