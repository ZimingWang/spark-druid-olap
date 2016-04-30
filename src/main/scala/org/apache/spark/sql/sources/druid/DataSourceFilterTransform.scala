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

package org.apache.spark.sql.sources.druid

import org.apache.spark.sql.sources._
import org.sparklinedata.druid._
import org.sparklinedata.druid.metadata.{DruidDimension, DruidMetric}

case class DruidFilterSpec(fs : FilterSpec,
                           spkColNm : String,
                           druidColNm : String)

object DataSourceFilterTransform {

  def apply(dsb : DruidSelectQueryBuilder,
                columns: Array[String],
                fils: Array[Filter]) : Option[DruidSelectQueryBuilder] = {
    implicit val iCE2: SparkIntervalConditionExtractor = new SparkIntervalConditionExtractor(dsb)

    for(
      dsb1 <- attributes(Some(dsb), columns);
      dsb2 <- filters(Some(dsb1), fils)
    )
      yield dsb2
  }

  private def intervalFilter(dsb : DruidSelectQueryBuilder,  f : Filter)(
    implicit iCE2: SparkIntervalConditionExtractor) :
  Option[DruidSelectQueryBuilder] = f match {
    case iCE2(iC) => dsb.interval(iC)
    case _ => None
  }

  private def attribute(odsb : Option[DruidSelectQueryBuilder],
                column : String) : Option[DruidSelectQueryBuilder] = {
      for(
        dsb <- odsb;
        dc <- dsb.druidColumn(column)
      ) yield {
        if (dc.isInstanceOf[DruidMetric]) {
          dsb.withMetric(dc.name)
        } else {
          dsb.withDimension(dc.name)
        }
      }
  }

  private def attributes(dsb : Option[DruidSelectQueryBuilder],
                requiredColumns: Array[String]) : Option[DruidSelectQueryBuilder] = {
    requiredColumns.foldLeft(dsb) { (dsB, c) =>
      dsB.flatMap { b =>
        attribute(Some(b), c)
      }
    }
  }

  private def filter(dsb : DruidSelectQueryBuilder, f : Filter)(
              implicit iCE2: SparkIntervalConditionExtractor): Option[DruidSelectQueryBuilder] =
    intervalFilter(dsb, f).orElse(
      dimFilter(dsb, f).map(f => dsb.filter(f))
    )

  private def dimFilter(dsb : DruidSelectQueryBuilder,
                f : Filter) : Option[FilterSpec] = f match {
    case EqualTo(attribute, value) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield new SelectorFilterSpec(dD.name, value.toString)
    }
    case LessThan(attribute, value) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield JavascriptFilterSpec.create(dD.name, "<", value.toString)
    }
    case LessThanOrEqual(attribute, value) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield JavascriptFilterSpec.create(dD.name, "<=", value.toString)
    }
    case GreaterThan(attribute, value) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield JavascriptFilterSpec.create(dD.name, ">", value.toString)
    }
    case GreaterThanOrEqual(attribute, value) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield JavascriptFilterSpec.create(dD.name, ">=", value.toString)
    }
    case StringStartsWith(attribute, value) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield new RegexFilterSpec(dD.name, s".*${value.toString}")
    }
    case StringEndsWith(attribute, value) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield new RegexFilterSpec(dD.name, s"${value.toString}.*")
    }
    case StringContains(attribute, value) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield new StringContainsSpec(dD.name, s"${value.toString}")
    }
    case In(attribute, vl) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension] )
        yield new ExtractionFilterSpec(dD.name, (for (e <- vl) yield e.toString()).toList)
    }
    case IsNull(attribute) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield new SelectorFilterSpec(dD.name, "")
    }
    case IsNotNull(attribute) => {
      for (dD <- dsb.druidColumn(attribute) if dD.isInstanceOf[DruidDimension])
        yield new NotFilterSpec("not", new SelectorFilterSpec(dD.name, ""))
    }
    case And(f1, f2) => {
      Utils.sequence(
        List(dimFilter(dsb, f1), dimFilter(dsb, f2))).map { args =>
        LogicalFilterSpec("and", args.toList)
      }
    }
    case Or(f1, f2) => {
      Utils.sequence(
        List(dimFilter(dsb, f1), dimFilter(dsb, f2))).map { args =>
        LogicalFilterSpec("or", args.toList)
      }
    }
    case Not(f) => {
      val fil = dimFilter(dsb, f)
      for (f <- fil)
        yield NotFilterSpec("not", f)
    }
    case _ => None
  }

  private def filters(dsb : Option[DruidSelectQueryBuilder], filters : Array[Filter])(
                       implicit iCE2: SparkIntervalConditionExtractor) :
  Option[DruidSelectQueryBuilder] = {
    filters.foldLeft(dsb) { (dsB, f) =>
      dsB.flatMap { b =>
        filter(b, f)
      }
    }
  }

}
