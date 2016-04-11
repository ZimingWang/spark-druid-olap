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

package org.sparklinedata.druid.JSCodeGen


private[JSCodeGen] case class JSDateTimeCtx(val tz_id: String, val ctx: JSCodeGenerator) {
  private[JSCodeGen] val tzVar: String = ctx.makeUniqueVarName
  private[JSCodeGen] val isoFormatterVar: String = ctx.makeUniqueVarName
  private[JSCodeGen] val isoFormatterWithTZVar: String = ctx.makeUniqueVarName

  private[JSCodeGen] var createJodaTZ: Boolean = false
  private[JSCodeGen] var createJodaISOFormatter: Boolean = false
  private[JSCodeGen] var createJodaISOFormatterWithTZ: Boolean = false

  private[JSCodeGen] def dateTimeInitCode: String = {
    var dtInitCode = ""
    if (createJodaTZ) {
      dtInitCode =
        s"""var ${tzVar} = new org.joda.time.DateTimeZone.forID("${tz_id}");""".stripMargin
    }
    if (createJodaISOFormatterWithTZ || createJodaISOFormatter) {
      dtInitCode += s"var ${isoFormatterVar} = " +
        s"org.joda.time.format.ISODateTimeFormat.dateTimeParser();"
    }
    if (createJodaISOFormatterWithTZ) {
      dtInitCode += s"var ${isoFormatterWithTZVar} = " +
        s"${isoFormatterVar}.withZone(${tzVar});"
    }

    dtInitCode
  }
}

private[JSCodeGen] object JSDateTimeCtx {
  private val dateFormat = "yyyy-MM-dd"
  private val timeStampFormat = "yyyy-MM-dd HH:mm:ss"
  private val mSecsInDay = 86400000

  private[JSCodeGen] def dtInFormatCode(f: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTZ = true
    s"""org.joda.time.format.forPattern($f).withZone(${ctx.tzVar})""".stripMargin
  }

  private[JSCodeGen] def dateToStrCode(jdt: String) =
    s"""($jdt.toString("$dateFormat"))""".stripMargin

  private[JSCodeGen] def stringToDateCode(ts: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaISOFormatter = true
    s"org.joda.time.LocalDate.parse(${ts}, ${ctx.isoFormatterVar})"
  }

  private[JSCodeGen] def noDaysToDateCode(ts: String) =
    s"new org.joda.time.LocalDate(${ts} * $mSecsInDay)"

  private[JSCodeGen] def dtToDateCode(ts: String) = s"${ts}.toLocalDate()"

  private[JSCodeGen] def dtToStrCode(jdt: String,
                                     f: String = s""""${timeStampFormat}"""".stripMargin) =
    s"""($jdt.toString(${f}))""".stripMargin

  private[JSCodeGen] def stringToDTCode(sd: String, f: String) =
    s"""org.joda.time.DateTime.parse($sd, $f)""".stripMargin

  private[JSCodeGen] def stringToISODTCode(sd: String, ctx: JSDateTimeCtx) = {
    ctx.createJodaTZ = true
    ctx.createJodaISOFormatterWithTZ = true
    stringToDTCode(s"""${sd}.replace(" ", "T")""".stripMargin, ctx.isoFormatterWithTZVar)
  }

  private[JSCodeGen] def longToISODTCode(l: Any, ctx: JSDateTimeCtx): String = {
    ctx.createJodaTZ = true
    ctx.createJodaISOFormatterWithTZ = true
    s"(new org.joda.time.DateTime($l, ${ctx.tzVar}))"
  }

  private[JSCodeGen] def localDateToDTCode(ld: String, ctx: JSDateTimeCtx): String = {
    ctx.createJodaTZ = true
    s"$ld.toDateTimeAtStartOfDay(${ctx.tzVar})"
  }

  private[JSCodeGen] def dtToIntegerCode(ts: String) = s"$ts.getMillis()"

  private[JSCodeGen] def dtToSecondsCode(ts: String) = s"Math.floor($ts.getMillis()/1000)"


  private[JSCodeGen] def dateAdd(dt: String, nd: String) = s"$dt.plusDays($nd)"

  private[JSCodeGen] def dateSub(dt: String, nd: String) = s"$dt.minusDays($nd)"

  private[JSCodeGen] def dateDiff(ed: String, sd: String) =
    s"org.joda.time.Days.daysBetween($sd, $ed).getDays()"

  private[JSCodeGen] def dTYear(dt: String) = s"$dt.getYear()"

  private[JSCodeGen] def dTQuarter(dt: String) = s"(Math.floor(($dt.getMonthOfYear() - 1) / 3) + 1)"

  private[JSCodeGen] def dTMonth(dt: String) = s"$dt.getMonthOfYear()"

  private[JSCodeGen] def dTDayOfMonth(dt: String) = s"$dt.getDayOfMonth()"

  private[JSCodeGen] def dTWeekOfYear(dt: String) = s"$dt.getWeekOfWeekyear()"

  private[JSCodeGen] def dTHour(dt: String) = s"$dt.getHourOfDay()"

  private[JSCodeGen] def dTMinute(dt: String) = s"$dt.getMinuteOfHour()"

  private[JSCodeGen] def dTSecond(ts: String) = s"$ts.getSecondOfMinute()"
}
