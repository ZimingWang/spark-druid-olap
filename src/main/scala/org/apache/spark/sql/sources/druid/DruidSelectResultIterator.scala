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

import java.io.InputStream

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, ObjectMapper}
import org.apache.commons.io.IOUtils
import org.apache.spark.util.NextIterator
import org.json4s._
import org.json4s.jackson.Json4sScalaModule
import org.json4s.jackson.JsonMethods._
import org.sparklinedata.druid.{CloseableIterator, SelectSpec, Utils}
import org.sparklinedata.druid.client._


private class DruidSelectResultIterator(is : InputStream,
                                        val selectSpec : SelectSpec,
                                        val druidQuerySvrConn : DruidQueryServerClient,
                                        initialOnDone : () => Unit)
  extends NextIterator[SelectResultRow] with CloseableIterator[SelectResultRow] {

  import Utils._

  val m = new OM()
  m.registerModule(new Json4sScalaModule)
  val jF = m.getFactory

  private var onDone = initialOnDone
  private var thisRoundHadData : Boolean = false
  private var jp : JsonParser = _
  private var ctxt: DeserializationContext = _
  private var jValDeser : JsonDeserializer[JValue]= _
  private var t : JsonToken = _
  private var currSelectResultContainerTS : String = _
  private var nextPagingIdentifiers : Map[String, Int] = _

  consumeNextStream(is)

  def consumeNextStream(is : InputStream) : Unit = {

    jp = jF.createParser(is)
    ctxt = m.createDeserializationContext(jp)
    jValDeser = m.jValueResultDeserializer(ctxt)

    jp.nextToken() // START_ARRAY

    // 1. SelResContainer
    t = jp.nextToken() // START_OBJECT

    // 1.1 timestamp
    t = jp.nextToken() // FIELD_NAME, jp.getCurrentName == timestamp
    t = jp.nextToken() // VALUE_STRING, set selResContainer.timestamp = jp.getText
    currSelectResultContainerTS = jp.getText

    t = jp.nextToken() // FIELD_NAME, jp.getCurrentName = "result"
    t= jp.nextToken() // START_OBJECT
    t = jp.nextToken() // FIELD_NAME , jp.getCurrentName == pagingIdentifiers

    t = jp.nextToken() // 1.2.1.v pIds value // START_OBJECT
    // t = jp.nextToken // FIELD_NAME
    val pagingIdentifiersJV : JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
    nextPagingIdentifiers = pagingIdentifiersJV.extract[ Map[String, Int]]

    // 1.2.2 events events field
    t = jp.nextToken // FIELD_NAME, jp.getCurrentName == events
    t = jp.nextToken // START_ARRAY
    t = jp.nextToken  // START_OBJECT
  }

  override protected def getNext(): SelectResultRow = {
    if ( t == JsonToken.END_ARRAY ) {
      if ( !thisRoundHadData ) {
        finished = true
        null
      } else {
        thisRoundHadData = false
        onDone
        val nextSelectSpec = selectSpec.withPagingIdentifier(nextPagingIdentifiers)
        val (r, nDone) = druidQuerySvrConn.executeSelectAsStream(nextSelectSpec)
        onDone = nDone
        consumeNextStream(r.getEntity.getContent)
        getNext
      }
    } else {
      val o : JsonAST.JValue = jValDeser.deserialize(jp, ctxt)
      val r = o.extract[SelectResultRow]
      t = jp.nextToken()
      thisRoundHadData = true
      r
    }
  }

  override protected def close(): Unit = {
    onDone
  }
}

private class DruidSelectResultIterator2(is : InputStream,
                                         val selectSpec : SelectSpec,
                                         val druidQuerySvrConn : DruidQueryServerClient,
                                         initialOnDone : () => Unit)
  extends NextIterator[SelectResultRow] with CloseableIterator[SelectResultRow] {

  import Utils._

  protected var thisRoundHadData : Boolean = false
  consumeNextStream(is)

  var onDone = initialOnDone
  var currResult : SelectResult = _
  var currIt : Iterator[SelectResultRow] = _

  private def consumeNextStream(is : InputStream) : Unit = {
    val s = IOUtils.toString(is)
    val jV = parse(s)
    currResult = jV.extract[SelectResultContainer].result
    currIt = currResult.events.iterator
  }

  override protected def getNext(): SelectResultRow = {
    if (currIt.hasNext) {
      thisRoundHadData = true
      currIt.next
    } else {
      if ( !thisRoundHadData ) {
        finished = true
        null
      } else {
        thisRoundHadData = false
        onDone
        val nextSelectSpec = selectSpec.withPagingIdentifier(currResult.pagingIdentifiers)
        val (r, nDone) = druidQuerySvrConn.executeSelectAsStream(nextSelectSpec)
        onDone = nDone
        consumeNextStream(r.getEntity.getContent)
        getNext
      }
    }
  }

  override protected def close(): Unit = onDone
}

object DruidSelectResultIterator {
  def apply(is : InputStream,
            selectSpec : SelectSpec,
            druidQuerySvrConn : DruidQueryServerClient,
            onDone : () => Unit,
            fromList : Boolean = false) : CloseableIterator[SelectResultRow] =
    if (!fromList) {
      new DruidSelectResultIterator(is, selectSpec, druidQuerySvrConn, onDone)
    }
    else {
      new DruidSelectResultIterator2(is, selectSpec, druidQuerySvrConn, onDone)
    }
}

