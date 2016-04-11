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

import org.apache.spark.Logging
import org.scalatest.BeforeAndAfterAll

class CodeGenTest extends BaseTest with BeforeAndAfterAll with Logging {

  test("gbexprtest1",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplier group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by bal",
    1,
    true, true)
  test("gbexprtest1B",
    "select sum(c_acctbal) as bal from orderLineItemPartSupplierBase group by " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000')" +
      " AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by bal",
    0,
    true, true)

  test("gbexprtest2",
    "select o_orderdate, " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') " +
      "AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) x," +
      "sum(c_acctbal) as bal from orderLineItemPartSupplier group by " +
      "o_orderdate, (substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate)," +
      " 'T00:00:00.000Z') AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by o_orderdate, x, bal",
    1,
    true, true)
  test("gbexprtest2B",
    "select o_orderdate, " +
      "(substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate), 'T00:00:00.000Z') " +
      "AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) x," +
      "sum(c_acctbal) as bal from orderLineItemPartSupplierBase group by " +
      "o_orderdate, (substr(CAST(Date_Add(TO_DATE(CAST(CONCAT(TO_DATE(o_orderdate)," +
      " 'T00:00:00.000Z') AS TIMESTAMP)), 5) AS TIMESTAMP), 0, 10)) order by o_orderdate, x, bal",
    0,
    true, true)
  test("gbexprtest3",
    "select o_orderdate, " +
      "(DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) as x " +
      "from orderLineItemPartSupplier group by o_orderdate, " +
      "cast('2015-07-21 10:10:10 PST' as date), " +
      " (DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) " +
      "order by o_orderdate, x",
    1,
    true, true)
  test("gbexprtest3B",
    "select o_orderdate, " +
      "(DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) as x " +
      "from orderLineItemPartSupplierBase group by o_orderdate, " +
      "cast('2015-07-21 10:10:10 PST' as date), " +
      " (DateDiff(cast(o_orderdate as date), cast('2015-07-21 10:10:10 PST' as date))) " +
      "order by o_orderdate, x",
    0,
    true, true)
  test("gbexprtest4",
    "select (Date_add(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplier group by " +
      "(Date_add(cast(o_orderdate as date), 360+3)) order by x",
    1,
    true, true)
  test("gbexprtest4B",
    "select (Date_add(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplierBase group by " +
      "(Date_add(cast(o_orderdate as date), 360+3)) order by x",
    0,
    true, true)
  test("gbexprtest5",
    "select (Date_sub(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplier group by " +
      "(Date_sub(cast(o_orderdate as date), 360+3)) order by x",
    1,
    true, true)
  test("gbexprtest5B",
    "select (Date_sub(cast(o_orderdate as date), 360+3))  as x " +
      "from orderLineItemPartSupplierBase group by " +
      "(Date_sub(cast(o_orderdate as date), 360+3)) order by x",
    0,
    true, true)
  test("gbexprtest6",
    "select o_orderdate, (weekofyear(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplier group by o_orderdate, " +
      "(weekofyear(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate",
    1,
    true, true)
  test("gbexprtest6B",
    "select o_orderdate, (weekofyear(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplierBase group by o_orderdate, " +
      "(weekofyear(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate",
    0,
    true, true)
  test("gbexprtest7",
    "select o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplier group by o_orderdate, " +
      "(unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate, x",
    1,
    true, true)
  test("gbexprtest7B",
    "select o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) as x " +
      "from orderLineItemPartSupplierBase group by o_orderdate, " +
      "(unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) order by o_orderdate, x",
    0,
    true, true)

  test("gbexprtest8",
    "SELECT   o_orderdate, Cast(Concat(Year(Cast(o_orderdate AS TIMESTAMP)), " +
      "(CASE WHEN Month(Cast(o_orderdate AS TIMESTAMP))<4 " +
      "THEN '-01' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<7 " +
      "THEN '-04' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<10 " +
      "THEN '-07' ELSE '-10'  END), '-01 00:00:00') " +
      "AS TIMESTAMP) AS x " +
      "FROM (SELECT * FROM   orderLineItemPartSupplier) m " +
      "GROUP BY o_orderdate, cast(concat(year(cast(o_orderdate AS timestamp)), " +
      "(CASE WHEN month(cast(o_orderdate AS timestamp))<4 THEN '-01' " +
      "WHEN month(cast(o_orderdate AS timestamp))<7 THEN '-04' " +
      "WHEN month(cast(o_orderdate AS timestamp))<10 " +
      "THEN '-07' ELSE '-10' END), '-01 00:00:00') AS timestamp)" +
      " order by o_orderdate, x ",
    1,
    true, true)
  test("gbexprtest8B",
    "SELECT   o_orderdate, Cast(Concat(Year(Cast(o_orderdate AS TIMESTAMP)), " +
      "(CASE WHEN Month(Cast(o_orderdate AS TIMESTAMP))<4 " +
      "THEN '-01' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<7 " +
      "THEN '-04' WHEN Month(Cast(o_orderdate AS TIMESTAMP))<10 " +
      "THEN '-07' ELSE '-10'  END), '-01 00:00:00') " +
      "AS TIMESTAMP) AS x " +
      "FROM (SELECT * FROM   orderLineItemPartSupplierBase) m " +
      "GROUP BY o_orderdate, cast(concat(year(cast(o_orderdate AS timestamp)), " +
      "(CASE WHEN month(cast(o_orderdate AS timestamp))<4 THEN '-01' " +
      "WHEN month(cast(o_orderdate AS timestamp))<7 THEN '-04' " +
      "WHEN month(cast(o_orderdate AS timestamp))<10 " +
      "THEN '-07' ELSE '-10' END), '-01 00:00:00') AS timestamp)" +
      " order by o_orderdate, x ",
    0,
    true, true)

  test("gbexprtest9",
    "select o_orderdate as x " +
      "from orderLineItemPartSupplier group by " +
      "o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1))) " +
      "order by o_orderdate, x",
    1,
    true, true)

  test("gbexprtest9B",
    "select o_orderdate as x " +
      "from orderLineItemPartSupplierBase group by " +
      "o_orderdate, (unix_timestamp(Date_Add(cast(o_orderdate as date), 1)))  " +
      "order by o_orderdate, x",
    0,
    true, true)


  test("gbexprtest10",
    " SELECT CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(o_orderdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplier) custom_sql_query " +
      "GROUP BY  " +
      "CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT), " +
      "YEAR(CAST(o_orderdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok",
    1,
    true, true)

  test("gbexprtest10B",
    " SELECT CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT) " +
      "AS `qr_row_hr_ok`, YEAR(CAST(o_orderdate AS TIMESTAMP)) AS `yr_row_hr_ok` " +
      "FROM ( select * from orderLineItemPartSupplierBase) custom_sql_query " +
      "GROUP BY  " +
      "CAST((MONTH(CAST(o_orderdate AS TIMESTAMP)) - 1) / 3 + 1 AS BIGINT), " +
      "YEAR(CAST(o_orderdate AS TIMESTAMP)) order by qr_row_hr_ok, yr_row_hr_ok",
    0,
    true, true)

  test("gbexprtest11",
    "select o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1)))) as x " +
      "from orderLineItemPartSupplier group by " +
      "o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1))))  " +
      "order by o_orderdate, x",
    1,
    true, true)
  test("gbexprtest11B",
    "select o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1)))) as x " +
      "from orderLineItemPartSupplierBase group by " +
      "o_orderdate, (from_unixtime(second(Date_Add(cast(o_orderdate as date), 1))))  " +
      "order by o_orderdate, x",
    0,
    true, true)
}
