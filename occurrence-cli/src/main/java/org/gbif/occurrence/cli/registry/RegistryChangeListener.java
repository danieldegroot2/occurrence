package org.gbif.occurrence.cli.registry;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;
import org.gbif.common.messaging.api.messages.RegistryChangeMessage;
import org.gbif.common.messaging.api.messages.StartCrawlMessage;
import org.gbif.occurrence.cli.registry.sync.OccurrenceScanMapper;
import org.gbif.occurrence.cli.registry.sync.SyncCommon;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens for any registry changes of interest to occurrences: namely organization and dataset updates or deletions.
 */
public class RegistryChangeListener extends AbstractMessageCallback<RegistryChangeMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(RegistryChangeListener.class);
  private static final int PAGING_LIMIT = 20;

  private static final Set<EndpointType> CRAWLABLE_ENDPOINT_TYPES = new ImmutableSet.Builder<EndpointType>()
    .add(EndpointType.BIOCASE, EndpointType.DIGIR, EndpointType.DIGIR_MANIS, EndpointType.TAPIR,
      EndpointType.DWC_ARCHIVE).build();

  private final MessagePublisher messagePublisher;
  private final OrganizationService orgService;

  public RegistryChangeListener(MessagePublisher messagePublisher, OrganizationService orgService) {
    this.messagePublisher = messagePublisher;
    this.orgService = orgService;
  }

  @Override
  public void handleMessage(RegistryChangeMessage message) {
    LOG.info("Handling registry [{}] msg for class [{}]", message.getChangeType(), message.getObjectClass());
    Class<?> clazz = message.getObjectClass();
    if ("Dataset".equals(clazz.getSimpleName())) {
      handleDataset(message.getChangeType(), (Dataset) message.getOldObject(), (Dataset) message.getNewObject());
    } else if ("Organization".equals(clazz.getSimpleName())) {
      handleOrganization(message.getChangeType(), (Organization) message.getOldObject(),
        (Organization) message.getNewObject());
    }
  }

  private void handleDataset(RegistryChangeMessage.ChangeType changeType, Dataset oldDataset, Dataset newDataset) {
    switch (changeType) {
      case DELETED:
        LOG.info("Sending delete for dataset [{}]", oldDataset.getKey());
        try {
          messagePublisher
            .send(new DeleteDatasetOccurrencesMessage(oldDataset.getKey(), OccurrenceDeletionReason.DATASET_MANUAL));
        } catch (IOException e) {
          LOG.warn("Could not send delete dataset message for key [{}]", oldDataset.getKey(), e);
        }
        break;
      case UPDATED:
        // as long as it has a crawlable endpoint, we need to crawl it no matter what changed
        if (isCrawlable(newDataset)) {
          LOG.info("Sending crawl for updated dataset [{}]", newDataset.getKey());
          try {
            messagePublisher.send(new StartCrawlMessage(newDataset.getKey()));
          } catch (IOException e) {
            LOG.warn("Could not send start crawl message for dataset key [{}]", newDataset.getKey(), e);
          }
          // check if owning org has changed, and update old records if so
          if (oldDataset.getPublishingOrganizationKey().equals(newDataset.getPublishingOrganizationKey())) {
            LOG.debug("Owning orgs match for updated dataset [{}] - taking no action", newDataset.getKey());
          } else {
            LOG.info("Starting m/r sync for changed owning org on dataset [{}]", newDataset.getKey());
            try {
              runMrSync(newDataset.getKey());
            } catch (Exception e) {
              LOG.warn("Failed to run RegistrySync m/r for dataset [{}]", newDataset.getKey(), e);
            }
          }
        } else {
          LOG.info("Ignoring update of dataset [{}] because no crawlable endpoints", newDataset.getKey());
        }
        break;
      case CREATED:
        if (isCrawlable(newDataset)) {
          LOG.info("Sending crawl for new dataset [{}]", newDataset.getKey());
          try {
            messagePublisher.send(new StartCrawlMessage(newDataset.getKey()));
          } catch (IOException e) {
            LOG.warn("Could not send start crawl message for dataset key [{}]", newDataset.getKey(), e);
          }
        } else {
          LOG.info("Ignoring creation of dataset [{}] because no crawlable endpoints", newDataset.getKey());
        }
        break;
    }
  }

  private static boolean isCrawlable(Dataset dataset) {
    boolean crawlable = false;
    for (Endpoint endpoint : dataset.getEndpoints()) {
      if (CRAWLABLE_ENDPOINT_TYPES.contains(endpoint.getType())) {
        crawlable = true;
        break;
      }
    }

    return crawlable;
  }

  private void handleOrganization(RegistryChangeMessage.ChangeType changeType, Organization oldOrg,
    Organization newOrg) {
    switch (changeType) {
      case DELETED:
        break;
      case UPDATED:
        if (!oldOrg.isEndorsementApproved() && newOrg.isEndorsementApproved()) {
          LOG.info("Starting crawl of all datasets for newly endorsed org [{}]", newOrg.getKey());
          DatasetVisitor visitor = new DatasetVisitor() {
            @Override
            public void visit(UUID datasetKey) {
              try {
                messagePublisher.send(new StartCrawlMessage(datasetKey));
              } catch (IOException e) {
                LOG.warn("Could not send start crawl message for newly endorsed dataset key [{}]", datasetKey, e);
              }
            }
          };
          visitOwnedDatasets(newOrg.getKey(), visitor);
        } else if (oldOrg.getCountry() != newOrg.getCountry() && newOrg.isEndorsementApproved()) {
          if (newOrg.getNumPublishedDatasets() > 0) {
            LOG.info("Starting m/r sync for all datasets of org [{}] because it has changed country from [{}] to [{}]",
              newOrg.getKey(), oldOrg.getCountry(), newOrg.getCountry());
            DatasetVisitor visitor = new DatasetVisitor() {
              @Override
              public void visit(UUID datasetKey) {
                runMrSync(datasetKey);
              }
            };
            visitOwnedDatasets(newOrg.getKey(), visitor);
          }
        }
        break;
      case CREATED:
        break;
    }
  }

  private static void runMrSync(@Nullable UUID datasetKey) {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.client.scanner.timeout.period", "600000");
    conf.set("hbase.regionserver.lease.period", "600000");
    conf.set("hbase.rpc.timeout", "600000");

    Scan scan = new Scan();
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.DK_COL);
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.HC_COL);
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.OOK_COL);
    scan.addColumn(SyncCommon.OCC_CF, SyncCommon.CI_COL);
    scan.setCaching(200);
    String rawDatasetKey = null;
    if (datasetKey != null) {
      rawDatasetKey = datasetKey.toString();
      scan.setFilter(new SingleColumnValueFilter(SyncCommon.OCC_CF, SyncCommon.DK_COL, CompareFilter.CompareOp.EQUAL,
          Bytes.toBytes(rawDatasetKey))
      );
    }

    Properties props = SyncCommon.loadProperties();
    String targetTable = props.getProperty(SyncCommon.OCC_TABLE_PROPS_KEY);

    String jobTitle = "Registry-Occurrence Sync on table " + targetTable;
    if (rawDatasetKey != null) {
      jobTitle = jobTitle + " for dataset " + rawDatasetKey;
    }

    try {
      Job job = new Job(conf, jobTitle);
      job.setJarByClass(OccurrenceScanMapper.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setNumReduceTasks(0);
      job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
      job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");


      if (targetTable == null) {
        LOG.error("Sync m/r not properly configured (occ table not set) - aborting");
      } else {
        TableMapReduceUtil
          .initTableMapperJob(targetTable, scan, OccurrenceScanMapper.class, ImmutableBytesWritable.class,
            NullWritable.class, job);
        job.waitForCompletion(true);
      }
    } catch (Exception e) {
      LOG.error("Could not start m/r job, aborting", e);
    }

    LOG.info("Finished running m/r sync for dataset [{}]", datasetKey);
  }

  private void visitOwnedDatasets(UUID orgKey, DatasetVisitor visitor) {
    int datasetCount = 0;
    boolean endOfRecords = false;
    int offset = 0;
    do {
      Pageable page = new PagingRequest(offset, PAGING_LIMIT);
      PagingResponse<Dataset> datasets = orgService.publishedDatasets(orgKey, page);

      for (Dataset dataset : datasets.getResults()) {
        visitor.visit(dataset.getKey());
      }
      datasetCount += datasets.getResults().size();
      offset += PAGING_LIMIT;

      if (datasets.isEndOfRecords()) {
        endOfRecords = datasets.isEndOfRecords();
      }
    } while (!endOfRecords);
    LOG.info("Visited [{}] datasets owned by org [{}]", datasetCount, orgKey);
  }

  private interface DatasetVisitor {

    void visit(UUID datasetKey);
  }
}
