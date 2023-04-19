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

import org.gbif.api.ws.mixin.Mixins;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessageRegistry;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.Data;
import lombok.SneakyThrows;

@Data
public class DownloadSparkCli {

  private final MessageListener listener;

  private final DownloadListenerConfiguration configuration;

  public static void main(String[] args) {
    DownloadSparkCli downloadSparkCli = new DownloadSparkCli(args[0]);
    downloadSparkCli.startUp();
  }

  @SneakyThrows
  public DownloadSparkCli(String configFileName) {
    configuration = readConfig(configFileName);
    MessageRegistry messageRegistry = new DefaultMessageRegistry();
    messageRegistry.register(DownloadsMessage.class, "occurrence", "occurrence.download");
    listener = new MessageListener(configuration.messaging.getConnectionParameters(), messageRegistry,
      createObjectMapper(), 1);
  }

  @SneakyThrows
  private static DownloadListenerConfiguration readConfig(String configFileName) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(new File(configFileName), DownloadListenerConfiguration.class);
  }

  @SneakyThrows
  public void startUp() {
    SparkSession sparkSession = createSparkSession();
    sparkSession.sql("USE dev");
    Dataset<Row> occurrence = sparkSession.table("occurrence").cache();
    System.out.println("Occurrence table cached " + occurrence.count());
    listener.listen(configuration.downloadQueueName, configuration.threadListeners,
      new MessageCallback<DownloadsMessage>() {
        @Override
        public void handleMessage(DownloadsMessage message) {
          if (message.getCommand().equalsIgnoreCase("flush")) {
            sparkSession.catalog().clearCache();
          } else if (message.getCommand().equalsIgnoreCase("query")) {
            Dataset<Row> result = sparkSession.sql(message.getQuery());
            System.out.println("Results");
            result.show(10);
          } else if (message.getCommand().equalsIgnoreCase("parse")) {
            try {
              LogicalPlan logicalPlan = sparkSession.sessionState().sqlParser().parsePlan(message.getQuery());
              System.out.println("Query is valid " + logicalPlan.prettyJson());
            } catch (ParseException ex) {
              System.out.println("Invalid query " + ex);
            }

          }
        }

        @Override
        public Class<DownloadsMessage> getMessageClass() {
          return DownloadsMessage.class;
        }

      });
    startAndWait();
  }

  @SneakyThrows
  private void startAndWait() {
    final CountDownLatch closeLatch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down!!");
        closeLatch.countDown();
      }
    });
    closeLatch.await();
  }


  private  SparkSession createSparkSession() {

    SparkConf sparkConf = new SparkConf()
      .set("spark.sql.warehouse.dir", configuration.hiveWarehouseDir);
    SparkSession.Builder sparkBuilder = SparkSession.builder()
      .appName("Occurrence Downloads")
      .config(sparkConf)
      .enableHiveSupport();
    return sparkBuilder.getOrCreate();
  }


  private ObjectMapper createObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Mixins.getPredefinedMixins().forEach(objectMapper::addMixIn);
    return objectMapper;
  }

}
