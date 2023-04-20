/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download;

import org.gbif.common.messaging.api.MessageCallback;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import scala.runtime.AbstractFunction0;

@Slf4j
@Data
public class DownloadMessageListener implements MessageCallback<DownloadsMessage> {
  private final SparkSession sparkSession;

  @Override
  public void handleMessage(DownloadsMessage message) {
    if (message.getCommand().equalsIgnoreCase("flush")) {
      flushCache();
    } else if (message.getCommand().equalsIgnoreCase("query")) {
      executeQuery(message);
    } else if (message.getCommand().equalsIgnoreCase("parse")) {
      parseQuery(message);
    }
  }

  private void flushCache() {
    sparkSession.catalog().clearCache();
    log.info("Spark Cache cleared");
  }
  private void executeQuery(DownloadsMessage message) {
    Dataset<Row> result = sparkSession.time(new AbstractFunction0<Dataset<Row>>() {
      @Override
      public Dataset<Row> apply() {
        return sparkSession.sql(message.getQuery());
      }
    });
    log.info("Results of query {}, {}", message.getQuery(), result.showString(10, 20, false));
  }

  private void parseQuery(DownloadsMessage message) {
    try {
      LogicalPlan logicalPlan = sparkSession.sessionState().sqlParser().parsePlan(message.getQuery());
      log.info("Query is valid {}", logicalPlan.prettyJson());
    } catch (ParseException ex) {
      log.error("Invalid query", ex);
    }
  }

  @Override
  public Class<DownloadsMessage> getMessageClass() {
    return DownloadsMessage.class;
  }
}
