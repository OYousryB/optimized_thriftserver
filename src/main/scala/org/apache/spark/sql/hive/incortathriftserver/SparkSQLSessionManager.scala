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

package org.apache.spark.sql.hive.incortathriftserver

import com.incorta.barq.logical.{PredefinedJoinQueryPlanOptimization, PredefinedJoinStrategy}
import com.incorta.hermes.plan.{FinalOptimization, HermesFilterOptimization, HermesStrategy, HermesViewOptimization, RequiredColumnsOptimization}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.incortathriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.incortathriftserver.server.SparkSQLOperationManager


private[hive] class SparkSQLSessionManager(hiveServer: HiveServer2, sqlContext: SQLContext, optimization: String)
  extends SessionManager(hiveServer)
  with ReflectedCompositeService {

  private lazy val sparkSqlOperationManager = new SparkSQLOperationManager()

  override def init(hiveConf: HiveConf) {
    setSuperField(this, "operationManager", sparkSqlOperationManager)
    super.init(hiveConf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      username: String,
      passwd: String,
      ipAddress: String,
      sessionConf: java.util.Map[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    val sessionHandle =
      super.openSession(protocol, username, passwd, ipAddress, sessionConf, withImpersonation,
          delegationToken)
    val session = super.getSession(sessionHandle)
    HiveThriftServer2.listener.onSessionCreated(
      session.getIpAddress, sessionHandle.getSessionId.toString, session.getUsername)
    val ctx = if (sqlContext.conf.hiveThriftServerSingleSession) {
      sqlContext
    } else {
      sqlContext.newSession()
    }
    ctx.setConf(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
    if (sessionConf != null && sessionConf.containsKey("use:database")) {
      ctx.sql(s"use ${sessionConf.get("use:database")}")
    }
    // Adding Custome Optimizations
    if (optimization == "barq") {
      println("Adding PredefinedJoinQueryPlanOptimization Optimization")
      ctx.experimental.extraOptimizations = ctx.experimental.extraOptimizations :+ PredefinedJoinQueryPlanOptimization

      println("Adding PredefinedJoinStrategy Strategy")
      ctx.experimental.extraStrategies = ctx.experimental.extraStrategies :+ PredefinedJoinStrategy
      }
    else if (optimization == "hermes") {
      println("Adding HermesViewOptimization Optimization")
      ctx.experimental.extraOptimizations = ctx.experimental.extraOptimizations :+ HermesViewOptimization

      println("Adding RequiredColumnsOptimization Optimization")
      ctx.experimental.extraOptimizations = ctx.experimental.extraOptimizations :+ RequiredColumnsOptimization

      println("Adding HermesFilterOptimization Optimization")
      ctx.experimental.extraOptimizations = ctx.experimental.extraOptimizations :+ HermesFilterOptimization

      println("Adding FinalOptimization Optimization")
      ctx.experimental.extraOptimizations = ctx.experimental.extraOptimizations :+ FinalOptimization

      println("Adding HermesStrategy Strategy")
      ctx.experimental.extraStrategies = ctx.experimental.extraStrategies :+ HermesStrategy
      }
    else {
      ctx.experimental.extraOptimizations = Seq()
      }

    sparkSqlOperationManager.sessionToContexts.put(sessionHandle, ctx)
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle) {
    HiveThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    super.closeSession(sessionHandle)
    sparkSqlOperationManager.sessionToActivePool.remove(sessionHandle)
    sparkSqlOperationManager.sessionToContexts.remove(sessionHandle)
  }
}
