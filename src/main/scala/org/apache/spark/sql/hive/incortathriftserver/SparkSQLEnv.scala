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

import java.io.PrintStream

import com.incorta.barq.logical.{PredefinedJoinQueryPlanOptimization, PredefinedJoinStrategy}
import com.incorta.hermes.plan.{FinalOptimization, HermesFilterOptimization, HermesStrategy, HermesViewOptimization, RequiredColumnsOptimization}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.{SQLContext, SparkSession, Strategy}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

/** A singleton object for the master program. The slaves should not access this. */
private[hive] object SparkSQLEnv extends Logging {
  logDebug("Initializing SparkSQLEnv")

  var sqlContext: SQLContext = _
  var sparkContext: SparkContext = _

  def init(optimization: String = "vanilla") {
    if (sqlContext == null) {
      val sparkConf = new SparkConf(loadDefaults = true)
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = sparkConf
        .getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
        .filterNot(_ == classOf[HiveThriftServer2].getName)

      sparkConf
        .setAppName(maybeAppName.getOrElse(s"$optimization SparkSQL::${Utils.localHostName()}"))

      val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
      sparkContext = sparkSession.sparkContext
      sqlContext = sparkSession.sqlContext

      if (optimization == "barq") {
        logInfo("Adding PredefinedJoinQueryPlanOptimization Optimization")
        sqlContext.experimental.extraOptimizations = sqlContext.experimental.extraOptimizations :+ PredefinedJoinQueryPlanOptimization

        logInfo("Adding PredefinedJoinStrategy Strategy")
        sqlContext.experimental.extraStrategies = sqlContext.experimental.extraStrategies :+ PredefinedJoinStrategy
      }
      else if (optimization == "hermes") {
        logInfo("Adding HermesViewOptimization Optimization")
        sqlContext.experimental.extraOptimizations = sqlContext.experimental.extraOptimizations :+ HermesViewOptimization

        logInfo("Adding RequiredColumnsOptimization Optimization")
        sqlContext.experimental.extraOptimizations = sqlContext.experimental.extraOptimizations :+ RequiredColumnsOptimization

        logInfo("Adding HermesFilterOptimization Optimization")
        sqlContext.experimental.extraOptimizations = sqlContext.experimental.extraOptimizations :+ HermesFilterOptimization

        logInfo("Adding FinalOptimization Optimization")
        sqlContext.experimental.extraOptimizations = sqlContext.experimental.extraOptimizations :+ FinalOptimization

        logInfo("Adding HermesStrategy Strategy")
        sqlContext.experimental.extraStrategies = sqlContext.experimental.extraStrategies :+ HermesStrategy
      }
      else {
        sqlContext.experimental.extraOptimizations = Seq()
      }

      val metadataHive = sparkSession
        .sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
      metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
      metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
      metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))
      sparkSession.conf.set(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
    }
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop() {
    logDebug("Shutting down Spark SQL Environment")
    // Stop the SparkContext
    if (SparkSQLEnv.sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      sqlContext = null
    }
  }

  def addStrategy(strategy: Strategy): Unit = {

    logInfo(s"Adding $strategy Strategy")
    sqlContext.experimental.extraStrategies = sqlContext.experimental.extraStrategies :+ strategy
  }

  def addOptimization(optimization: Rule[LogicalPlan]): Unit = {
    logInfo(s"Adding $optimization Optimization")
    sqlContext.experimental.extraOptimizations = sqlContext.experimental.extraOptimizations :+ optimization

  }

}
