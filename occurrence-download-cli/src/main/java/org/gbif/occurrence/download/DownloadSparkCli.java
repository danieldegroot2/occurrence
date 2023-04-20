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
import org.gbif.common.messaging.api.MessageRegistry;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
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
    listener = createListener(configuration);
  }

  @SneakyThrows
  private static MessageListener createListener(DownloadListenerConfiguration configuration) {
    MessageRegistry messageRegistry = new DefaultMessageRegistry();
    messageRegistry.register(DownloadsMessage.class, configuration.exchange, configuration.routingKey);
    return new MessageListener(configuration.messaging.getConnectionParameters(), messageRegistry,
      createObjectMapper(), 1);
  }

  @SneakyThrows
  private static DownloadListenerConfiguration readConfig(String configFileName) {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(new File(configFileName), DownloadListenerConfiguration.class);
  }

  private void cacheTable(SparkSession sparkSession, String tableName) {
    sparkSession.sqlContext().cacheTable(tableName);
    Dataset<Row> count = sparkSession.sql("select count(*) from " + tableName);
    log.info("{} table cached with record count of {}",  tableName, count.showString(1, 20, false));
  }

  @SneakyThrows
  public void startUp() {
    SparkSession sparkSession = createSparkSession();

    useDatabase(sparkSession, configuration.database);

    cacheTable(sparkSession, configuration.sourceTable);

    listen(sparkSession);

    startAndWait(sparkSession);
  }

  private void useDatabase(SparkSession sparkSession, String database) {
    sparkSession.sql("USE " + database);
  }

  @SneakyThrows
  private void listen(SparkSession sparkSession) {
    listener.listen(configuration.downloadQueueName, configuration.threadListeners, new DownloadMessageListener(sparkSession));
  }

  @SneakyThrows
  private void startAndWait(SparkSession sparkSession) {
    CountDownLatch closeLatch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Shutting down Downloads CLI");
      closeLatch.countDown();
      sparkSession.close();
    }));
    closeLatch.await();
  }


  private  SparkSession createSparkSession() {

    SparkConf sparkConf = new SparkConf();
    SparkSession.Builder sparkBuilder = SparkSession.builder()
      .appName("Occurrence Downloads")
      .config(sparkConf)
      .enableHiveSupport();
    return sparkBuilder.getOrCreate();
  }


  private static ObjectMapper createObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    Mixins.getPredefinedMixins().forEach(objectMapper::addMixIn);
    return objectMapper;
  }

}
