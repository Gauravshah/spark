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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.AnalysisException
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.NESTED_SCHEMA_PRUNING_ENABLED

/**
  * A PlanTest that ensures that all tests in this suite are run with nested schema pruning enabled.
  * Remove this trait once the default value of SQLConf.NESTED_SCHEMA_PRUNING_ENABLED is set to true.
  */
private[sql] trait SchemaPruningTest extends PlanTest with BeforeAndAfterAll {
  private var originalConfSchemaPruningEnabled = false


  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      if (SQLConf.staticConfKeys.contains(k)) {
        throw new AnalysisException(s"Cannot modify the value of a static config: $k")
      }
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  override protected def beforeAll(): Unit = {
    // Call `withSQLConf` eagerly because some subtypes of `PlanTest` (I'm looking at you,
    // `SQLTestUtils`) override `withSQLConf` to reset the existing `SQLConf` with a new one without
    // copying existing settings first. This here is an awful, ugly way to get around that behavior
    // by initializing the "real" `SQLConf` with an noop call to `withSQLConf`. I don't want to risk
    // "fixing" the downstream behavior, breaking everything else that's expecting these semantics.
    // Oh well...
    withSQLConf()(())
    originalConfSchemaPruningEnabled = conf.nestedSchemaPruningEnabled
    conf.setConf(NESTED_SCHEMA_PRUNING_ENABLED, true)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      conf.setConf(NESTED_SCHEMA_PRUNING_ENABLED, originalConfSchemaPruningEnabled)
    }
  }
}