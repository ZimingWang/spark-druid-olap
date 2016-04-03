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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.sparklinedata.druid.metadata._
import scala.collection.mutable
import scala.language.reflectiveCalls

case class JSCode(jsFn: String, inParams: List[String])

object JSCodeGenUtils extends Logging {
  def genJSFn(dqb: DruidQueryBuilder,
              e: Expression, mulInParamsAllowed: Boolean): Option[JSCode] = {
    val cGen = new JSExprCodeGen(dqb, mulInParamsAllowed)
    for (fnb <- cGen.genExprCode(e) if cGen.inParams.nonEmpty;
         rStmt <- cGen.genCastExprCode(fnb, StringType)) yield {
      val fnParams: List[String] = cGen.inParams.toList
      JSCode(
        s"""function (${fnParams.mkString(", ")}) {
            ${fnb.linesSoFar}
            ${rStmt.linesSoFar}

            return(${rStmt.getCodeRef});
            }""".stripMargin,
        fnParams)
    }
  }
}


private case class JSExprCode(val fnVar: Option[String], val linesSoFar: String,
                              val curLine: String, val fnDT: DataType) {
  def this(curLine: String, fnDT: DataType) = this(None, "", curLine, fnDT)

  def getCodeRef: String = {
    if (fnVar.isDefined) fnVar.get else curLine
  }
}

private class JSExprCodeGen(dqb: DruidQueryBuilder, mulInParamsAllowed: Boolean) {
  var inParams: mutable.HashSet[String] = mutable.HashSet()
  private var toDateVar: mutable.HashSet[String] = mutable.HashSet()
  private var unSupportedDateVar: mutable.HashMap[String, JSExprCode] = mutable.HashMap()

  private var uid: Int = 0

  def makeUniqueVarName: String = {
    uid += 1
    "v" + uid
  }

  def genExprCode(e: Expression): Option[JSExprCode] = {
    e match {
      case AttributeReference(nm, dT, _, _) =>
        for (dD <- dqb.druidColumn(nm)
             if (dD.isInstanceOf[DruidDimension] || dD.isInstanceOf[DruidTimeDimension]) &&
               validInParams(nm)) yield {
          val v = if (dD.isInstanceOf[DruidTimeDimension]) dqb.druidColumn(nm).get.name else nm
          new JSExprCode(v, e.dataType)
        }
      case Literal(value, dataType) => {
        var valStr: Option[String] = None
        dataType match {
          case IntegerType | LongType | ShortType | DoubleType | FloatType =>
            valStr = Some(value.toString)
          case StringType => valStr = Some(s""""${e.toString}"""")
          case NullType => valStr = Some("null")
          case DateType if value.isInstanceOf[Int] =>
            valStr = Some(s"(new Date($value * 86400000))")
          case _ => valStr =
            for (ce <- genCastExprCode(new JSExprCode(value.toString, StringType), dataType))
              yield ce.getCodeRef
        }
        if (valStr.nonEmpty) Some(new JSExprCode(valStr.get, dataType)) else None
      }
      case Cast(s, dt) => Some(genCastExprCode(s, dt)).flatten

      case Concat(eLst) =>
        // TODO: If second gencode failed then this should return none
        // TODO: Use FoldLeft
        var cJSFn: Option[JSExprCode] = None
        for (ex <- eLst; exCode <- genExprCode(ex)) {
          if (cJSFn.isEmpty) {
            cJSFn = Some(exCode)
          } else {
            cJSFn = Some(JSExprCode(None,
              s"${cJSFn.get.linesSoFar} ${exCode.linesSoFar} ",
              s"((${cJSFn.get.getCodeRef}).concat(${exCode.getCodeRef}))",
              StringType))
          }
        }
        cJSFn
      case Upper(u) =>
        for (exCode <- genExprCode(u) if u.dataType.isInstanceOf[StringType]) yield
          JSExprCode(None,
            exCode.linesSoFar, s"(${exCode.getCodeRef}).toUpperCase()", StringType)
      case Lower(l) =>
        for (exCode <- genExprCode(l) if l.dataType.isInstanceOf[StringType]) yield
          JSExprCode(None,
            exCode.linesSoFar, s"(${exCode.getCodeRef}).toLowerCase()", StringType)
      case Substring(str, pos, len) =>
        for (strcode <- genExprCode(str); posL <- genExprCode(pos)
             if pos.isInstanceOf[Literal]; lenL <- genExprCode(len)
             if len.isInstanceOf[Literal]) yield
          JSExprCode(None, s"${strcode.linesSoFar} ${posL.linesSoFar}",
            s"(${strcode.getCodeRef}).substring(${posL.getCodeRef}, ${lenL.getCodeRef})",
            StringType)
      case Coalesce(le) => {
        val l = le.flatMap(e => genExprCode(e))
        if (le.size == l.size) {
          Some(l.foldLeft(new JSExprCode("", StringType))((a, j) =>
            JSExprCode(a.fnVar.filter(_.nonEmpty).orElse(j.fnVar),
              a.linesSoFar + j.linesSoFar,
              if (a.getCodeRef.isEmpty) "" else "||" + s"((${j.getCodeRef}) != null)", j.fnDT)))
        } else {
          None
        }
      }

      case CaseWhen(le) => {
        val l = le.flatMap(e => genExprCode(e))
        if (l.length == le.length) {
          val v1 = makeUniqueVarName
          val jv = ((0 until l.length / 2).foldLeft(
            JSExprCode(Some(v1), s"var ${v1} = false;", "", BooleanType)) { (a, i) =>
            val cond = l(i * 2)
            val res = l(i * 2 + 1)
            JSExprCode(a.fnVar, a.linesSoFar +
              s"""
                  ${cond.linesSoFar}
              if (!${v1} && (${cond.getCodeRef})) {
              ${res.linesSoFar}
              ${v1} = ${res.getCodeRef};
        }""".stripMargin, "", a.fnDT)
          })
          val de = l(l.length - 1)
          Some(JSExprCode(jv.fnVar, jv.linesSoFar +
            s"""
              if (!${v1}) {
              ${de.linesSoFar}
              ${v1} = ${de.getCodeRef};
        }""".stripMargin, "", de.fnDT))
        } else {
          None
        }
      }
      case LessThan(l, r) => genComparisonCode(l, r, " < ")
      case LessThanOrEqual(l, r) => genComparisonCode(l, r, " <= ")
      case GreaterThan(l, r) => genComparisonCode(l, r, " > ")
      case GreaterThanOrEqual(l, r) => genComparisonCode(l, r, " >= ")
      case EqualTo(l, r) => genComparisonCode(l, r, " == ")
      case And(l, r) => genComparisonCode(l, r, " && ")
      case Or(l, r) => genComparisonCode(l, r, " || ")
      case Not(e) => {
        for (j <- genExprCode(e)) yield
          JSExprCode(None, j.linesSoFar, s"!(${j.getCodeRef})", BooleanType)
      }

      case ToDate(de) =>
        for (fn <- genExprCode(de); dtFn <- getJSDateExprCode(fn.fnDT, fn.getCodeRef)) yield {
          val v1 = makeUniqueVarName
          val v2 = makeUniqueVarName
          toDateVar += v2
          JSExprCode(Some(v2),
            fn.linesSoFar +
              s"""| var $v1 = $dtFn;
                  | var $v2 = new Date($v1.getFullYear(), $v1.getMonth(), $v1.getDate());
            """.stripMargin, "", DateType)
        }
      case DateAdd(sd, d) => getDateDayArithmetic(sd, d, "+")
      case DateSub(sd, d) => getDateDayArithmetic(sd, d, "-")
      case DateDiff(ed, sd) => {
        for (edE <- genExprCode(ed); sdE <- genExprCode(sd)) yield {
          JSExprCode(None, edE.linesSoFar + sdE.linesSoFar,
            s"Math.floor(((${edE.getCodeRef}) - (${sdE.getCodeRef}))/86400000)", IntegerType)
        }
      }
      case Year(y) => getDateFieldsCode(y, "getFullYear()", IntegerType)
      case Quarter(q) => {
        for (qE <- genExprCode(q)) yield {
          JSExprCode(None, qE.linesSoFar, s"(Math.ceil(((${qE.getCodeRef}).getMonth()) / 3))",
            IntegerType)
        }
      }
      case Month(mo) => getDateFieldsCode(mo, "getMonth()", IntegerType)
      case DayOfMonth(d) => getDateFieldsCode(d, "getDate()", IntegerType)
      case WeekOfYear(w) => {
        for (wE <- genExprCode(w)) yield {
          val v1 = makeUniqueVarName
          val v2 = makeUniqueVarName
          JSExprCode(Some(v2),
            wE.linesSoFar +
              s"""
                 | var ${v1} = ${wE.getCodeRef};
                 |var ${v2} = (Math.ceil(((((${v1}) -
                 |(new Date(${v1}.getFullYear(),0,1)))/8.64e7)+1)/7))""".stripMargin,
            "", IntegerType)
        }
      }
      case Hour(h) => getDateFieldsCode(h, "getHours()", IntegerType)
      case Minute(m) => getDateFieldsCode(m, "getMinutes()", IntegerType)
      case Second(s) => getDateFieldsCode(s, "getSeconds()", IntegerType)
      case FromUnixTime(s, f) if f.isInstanceOf[Literal] =>
        for (sE <- genExprCode(s); fdE <- getFormattedDateStr(sE, f.toString())) yield
          JSExprCode(fdE.fnVar, sE.linesSoFar + fdE.linesSoFar, fdE.getCodeRef, StringType)
      case UnixTimestamp(t, f) if f.isInstanceOf[Literal] => {
        for (tE <- genCastExprCode(t, DateType)) yield {
          val d = if (tE.fnDT.isInstanceOf[DateType]) {
            s"Math.floor((${tE.getCodeRef}).getTime()/1000)"
          } else {
            s"((new Date(${tE.getCodeRef})).getTime()/1000)"
          }
          JSExprCode(None, tE.linesSoFar, d, e.dataType)
        }
      }

      case Add(l, r) => Some(genBArithmeticExprCode(l, r, e, "+")).flatten
      case Subtract(l, r) => Some(genBArithmeticExprCode(l, r, e, "-")).flatten
      case Multiply(l, r) => Some(genBArithmeticExprCode(l, r, e, "*")).flatten
      case Divide(l, r) => Some(genBArithmeticExprCode(l, r, e, "/")).flatten
      case Remainder(l, r) => Some(genBArithmeticExprCode(l, r, e, "%")).flatten
      case _ => None.flatten
    }
  }

  private def genBArithmeticExprCode(l: Expression, r: Expression, ce: Expression,
                                     op: String): Option[JSExprCode] = {
    for (lc <- genExprCode(l); rc <- genExprCode(r)) yield
      JSExprCode(None,
        s"${lc.linesSoFar} ${rc.linesSoFar} ",
        s"((${lc.getCodeRef}) $op (${rc.getCodeRef}))", ce.dataType)
  }

  private def genComparisonCode(l: Expression, r: Expression, op: String): Option[JSExprCode] = {
    for (le <- genExprCode(l); re <- genExprCode(r)) yield
      JSExprCode(le.fnVar.filter(_.nonEmpty).orElse(re.fnVar),
        le.linesSoFar + re.linesSoFar,
        s"(${le.getCodeRef}) ${op} (${re.getCodeRef})", BooleanType)
  }

  private def genCastExprCode(e: Expression, dt: DataType): Option[JSExprCode] = {
    for (fn <- genExprCode(e);
         cs <- genCastExprCode(fn, dt)) yield
      JSExprCode(cs.fnVar, fn.linesSoFar + cs.linesSoFar, cs.curLine, dt)
  }

  def genCastExprCode(je: JSExprCode, outDT: DataType): Option[JSExprCode] = {
    val inDT = je.fnDT
    val inExprStr = je.getCodeRef
    val inExprStrVName = je.fnVar.nonEmpty
    if (TypeUtils.checkForSameTypeInputExpr(List(inDT, outDT), "JSCastFN arg checks").isFailure) {
      outDT match {
        case FloatType | DoubleType =>
          inDT match {
            case StringType => Some(new JSExprCode(s"parseFloat($inExprStr)", outDT))
            case FloatType | DoubleType | IntegerType | LongType | ShortType =>
              Some(new JSExprCode(s"Number($inExprStr)", outDT))
            case _ => None
          }
        case IntegerType | LongType | ShortType =>
          inDT match {
            case StringType => Some(new JSExprCode(s"parseInt($inExprStr)", outDT))
            case FloatType | DoubleType | IntegerType | LongType | ShortType =>
              Some(new JSExprCode(s"Number($inExprStr)", outDT))
            case _ => None
          }
        case BooleanType => Some(new JSExprCode(s"Boolean(${inExprStr})", outDT))
        case StringType =>
          inDT match {
            case FloatType | DoubleType | IntegerType | LongType | ShortType =>
              Some(new JSExprCode(s"($inExprStr).toString()", outDT))
            case TimestampType => getFormattedDateStr(je, "YYYY-MM-DD hh:mm:ss")
            case DateType => {
              // This is a hack. TO_DATE is supposed to get only date pieces (except time)
              // However Date.toISOString adds 00:00:00. Here we remove it
              val v = if (inExprStrVName && toDateVar.contains(inExprStr)) ".slice(0, 10)" else ""
              Some(new JSExprCode(s"($inExprStr).toISOString()${v}", outDT))
            }
            case _ => None
          }
        case TimestampType =>
          inDT match {
            case DateType => Some(new JSExprCode(s"($inExprStr)", outDT))
            case IntegerType | LongType | ShortType =>
              Some(new JSExprCode(s"(new Date($inExprStr))", outDT))
            case StringType => {
              var ls = ""
              var cl = ""
              if (inExprStrVName && unSupportedDateVar.contains(inExprStr)) {
                val je = unSupportedDateVar.get(inExprStr).get
                ls = je.linesSoFar
                cl = s"((${je.getCodeRef}))"
              } else {
                val v = getJSDateStr(inExprStr)
                if (v.nonEmpty) {
                  cl = s"((new Date(${v.get})))"
                } else {
                  None
                }
              }

              if (cl.nonEmpty) {
                Some(new JSExprCode(None, ls, cl, outDT))
              } else {
                None
              }
            }
            case _ => None
          }
        case DateType =>
          inDT match {
            case StringType => {
              if (inExprStrVName && unSupportedDateVar.contains(inExprStr)) {
                unSupportedDateVar.get(inExprStr)
              } else {
                val v = getJSDateStr(inExprStr)
                if (v.nonEmpty) {
                  Some(new JSExprCode(s"(new Date(${v.get}))", outDT))
                } else {
                  None
                }
              }
            }
            case IntegerType | LongType =>
              Some(new JSExprCode(s"(new Date(parseFloat($inExprStr)))", outDT))
            case TimestampType => Some(new JSExprCode(s"($inExprStr)", outDT))
            case _ => None
          }
        case _ => None
      }
    } else {
      Some(new JSExprCode(inExprStr, outDT))
    }
  }

  private def getJSDateExprCode(exDT: DataType, exJSStr: String): Option[String] = {
    exDT match {
      case DateType | TimestampType => Some(exJSStr)
      case StringType | IntegerType | LongType | ShortType =>
        Some(s"(new Date($exJSStr))")
      case _ => None
    }
  }

  private def getDateFieldsCode(e: Expression, f: String, outDT: DataType): Option[JSExprCode] = {
    for (sdE <- genExprCode(e); ds <- getJSDateExprCode(e.dataType, sdE.getCodeRef)) yield {
      JSExprCode(None, sdE.linesSoFar,
        s"(${ds})." + f, outDT)
    }
  }

  private def validInParams(inParam: String): Boolean = {
    if (!mulInParamsAllowed && ((inParams += inParam).size > 1)) false else true
  }

  private def getDateDayArithmetic(sd: Expression, d: Expression, op: String):
  Option[JSExprCode] = {
    for (sdE <- genExprCode(sd); dE <- genExprCode(d)) yield {
      val v1 = makeUniqueVarName
      val v1Rhs = if (sd.dataType.isInstanceOf[DateType]) s"${sdE.getCodeRef}"
      else s"new Date(${sdE.getCodeRef})"
      JSExprCode(Some(v1),
        s"""
           |${sdE.linesSoFar} ${dE.linesSoFar}
           |var ${v1} = ${v1Rhs};
           |${v1}.setDate(${v1}.getDate() ${op} (${dE.getCodeRef}));
      """.stripMargin, "", DateType)
    }
  }

  /**
    * Get date string formatted in the specified format
    *
    * @param sE
    * @param f Format needed
    * @return Fomatted string or None
    */
  private def getFormattedDateStr(sE: JSExprCode, f: String): Option[JSExprCode] = {
    // TODO: Explore JS Intl.DateTimeFormat
    val iV = makeUniqueVarName
    val yv = makeUniqueVarName
    val mv = makeUniqueVarName
    val dv = makeUniqueVarName
    val hv = makeUniqueVarName
    val miv = makeUniqueVarName
    val sv = makeUniqueVarName
    val fV = makeUniqueVarName
    val isoAdjV = makeUniqueVarName


    val y = s"""var ${yv} = ${iV}.getFullYear().toString();""".stripMargin
    val mo =
      s"""var ${mv} = (${iV}.getMonth() + 1).toString();
         |if (${mv}.length == 1) ${mv} = "0".concat(${mv});""".stripMargin
    val da =
      s"""var ${dv} = ${iV}.getDate().toString();
         |if (${dv}.length == 1) ${dv} = "0".concat(${dv});""".stripMargin
    val h =
      s"""var ${hv} = ${iV}.getHours().toString();
         |if (${hv}.length == 1) ${hv} = "0".concat(${hv});""".stripMargin
    val mi =
      s"""var ${miv} = ${iV}.getMinutes().toString();
         |if (${miv}.length == 1) ${miv} = "0".concat(${miv});""".stripMargin
    val s =
      s"""var ${sv} = ${iV}.getSeconds().toString();
         |if (${sv}.length == 1) ${sv} = "0".concat(${sv});""".stripMargin

    var df = s"""${y}
                |${mo}
                |${da}""".stripMargin
    var tf = s"""${h}
                |${mi}
                |${s}""".stripMargin
    var strExtract: Option[String] = None
    var concatStr: Option[String] = None
    var isoDAdj = ""

    f match {
      case ci"YYYY-MM-DD hh:mm:ss" => {
        strExtract = Some(df + tf)
        concatStr = Some(
          s"""${yv}.concat("-").concat(${mv}).concat("-")""" +
            s""".concat(${dv}).concat(" ").concat(${hv}).concat(":").concat(${miv})""" +
            s""".concat(":").concat(${sv})""")
        isoDAdj = s"${isoAdjV}.setMilliseconds(0)"
      }
      case ci"YYYY-MM-DD" => {
        strExtract = Some(df)
        concatStr = Some(s"""${y}.concat("-").concat(${mo}).concat("-").concat(${da})""")
        isoDAdj = s"${isoAdjV}.setHours(0,0,0,0)"
      }
      case ci"MM-dd-YY" => {
        strExtract = Some(df)
        concatStr = Some(s"""${mv}.concat("-").concat(${dv}).concat("-").concat(${yv})""")
        isoDAdj = s"${isoAdjV}.setHours(0,0,0,0)"
      }
      case ci"YYYY/MM/DD hh:mm:ss" => {
        strExtract = Some(df + tf)
        concatStr = Some(
          s"""${yv}.concat("/").concat(${mv}).concat("/")""" +
            s""".concat(${dv}).concat(" ").concat(${hv}).concat(":").concat(${miv})""" +
            s""".concat(":").concat(${sv})""")
        isoDAdj = s"${isoAdjV}.setMilliseconds(0)"
      }
      case ci"YYYY/MM/DD" => {
        strExtract = Some(df)
        concatStr = Some(s"""${yv}.concat("/").concat(${mv}).concat("/").concat(${dv})""")
        isoDAdj = s"${isoAdjV}.setHours(0,0,0,0)"
      }
      case ci"MM/DD/YY" => {
        strExtract = Some(df)
        concatStr = Some(s"""${mv}.concat("/").concat(${dv}).concat("/").concat(${yv})""")
        isoDAdj = s"${isoAdjV}.setHours(0,0,0,0)"
      }
    }

    if (concatStr.nonEmpty) {
      if (isoDAdj.nonEmpty) {
        val isoDE = JSExprCode(Some(isoAdjV),
          s"""
             |var ${isoAdjV} = new Date(${iV});
             |${isoDAdj};
           """.stripMargin, "", DateType)
        unSupportedDateVar.put(fV, isoDE)
      }

      Some(JSExprCode(Some(fV),
        s"""
           |var ${iV} = ${sE.getCodeRef};
           |${strExtract.get}
           |var ${fV} = ${concatStr.get};""".stripMargin, "", StringType))
    } else {
      None
    }
  }

  // TODO: Add formats for rfc2822 & also use String Interpolation
  // NOTE: currently we assume
  // 1. any literal of the formm " dd:dd:dd" is time stamp
  // 2. " dd:dd:dd" is the only form of time literal that will be generated by Tableau
  private val TPattern = "\\d{2}:\\d{2}:\\d{2}".r
  private val ISOTPattern1 = "T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z".r
  private val TableuTPattern1 = "\\ \\d{2}:\\d{2}:\\d{2}".r

  private def getJSDateStr(s: String): Option[String] = {
    var fs: Option[String] = Some(s)
    for (e <- TableuTPattern1.findFirstIn(fs.get)) {
      val lv = fs.get.split(e)
      if (lv.size == 2) {
        fs = Some(lv(0) + "T" + e.trim + ".000Z" + lv(1))
      }
    }
    fs
  }

  implicit class StringInterpolations(sc: StringContext) {
    def ci = new {
      def unapply(other: String) = sc.parts.mkString.equalsIgnoreCase(other)
    }
  }

}
