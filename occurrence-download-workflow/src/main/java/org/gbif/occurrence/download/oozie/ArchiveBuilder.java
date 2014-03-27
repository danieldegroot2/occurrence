package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.common.InterpretedEnum;
import org.gbif.api.model.registry.Citation;
import org.gbif.api.model.registry.Contact;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.model.registry.eml.DataDescription;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.ContactType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.Language;
import org.gbif.occurrence.common.download.DownloadException;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.util.DwcArchiveUtils;
import org.gbif.occurrence.download.util.HeadersFileUtil;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.registry.metadata.EMLWriter;
import org.gbif.utils.file.CompressionUtil;
import org.gbif.utils.file.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.download.util.DwcDownloadsConstants.CITATIONS_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.INTERPRETED_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.METADATA_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.MULTIMEDIA_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.RIGHTS_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.VERBATIM_FILENAME;

/**
 * Creates a dwc archive for occurrence downloads based on the hive query result files generated
 * during the Oozie workflow. It create a local archive folder with an occurrence data file and a dataset subfolder
 * that contains an EML metadata file per dataset involved.
 */
public class ArchiveBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveBuilder.class);

  /**
   * Simple, local representation for a constituent dataset.
   */
  static class Constituent {

    private final String title;
    private final int records;

    Constituent(String title, int records) {
      this.title = title;
      this.records = records;
    }
  }

  // 0 is used for the headers filename because it will be the first file to be merged when creating the occurrence data
  // file using the copyMerge function
  private static final String HEADERS_FILENAME = "0";

  // The CRC is created by the function FileSyste.copyMerge function
  private static final String CRC_FILE_FMT = ".%s.crc";
  private static final String DOWNLOAD_CONTACT_SERVICE = "GBIF Download Service";
  private static final String METADATA_DESC_HEADER_FMT =
    "A dataset containing all occurrences available in GBIF matching the query: %s"
      +
      "<br/>The dataset includes records from the following constituent datasets. The full metadata for each constituent is also included in this archive:<br/>";
  private static final String CITATION_HEADER =
    "Please cite this data as follows, and pay attention to the rights documented in the rights.txt: \n"
      + "Please respect the rights declared for each dataset in the download: ";
  private static final String DATASET_TITLE_FMT = "GBIF Occurrence Download %s";
  private static final String DATA_DESC_FORMAT = "Darwin Core Archive";

  private static final List<ContactType> AUTHOR_TYPES = ImmutableList.of(
    ContactType.ORIGINATOR, ContactType.AUTHOR, ContactType.POINT_OF_CONTACT);
  private static final Splitter TAB_SPLITTER = Splitter.on('\t').trimResults();
  private final DatasetService datasetService;
  private final DatasetOccurrenceDownloadUsageService datasetUsageService;
  private final Dataset dataset;
  private final File archiveDir;
  private final String downloadId;
  private final String user;
  private final String query;
  private final String interpretedDataTable;
  private final String verbatimDataTable;
  private final String multimediaDataTable;
  private final String citationTable;
  // HDFS related
  private final Configuration conf;
  private final FileSystem hdfs;
  private final FileSystem localfs;
  private final String hdfsPath;
  private final URL downloadLink;
  private final List<Constituent> constituents = Lists.newArrayList();
  private final boolean isSmallDownload;

  private final Ordering<Constituent> constituentsOrder = Ordering.natural().onResultOf(
    new Function<Constituent, Integer>() {

      public Integer apply(Constituent c) {
        return c.records;
      }
    });

  /**
   * @param datasetService
   * @param user
   * @param query
   * @param downloadId
   * @param conf
   * @param hdfs
   * @param localfs
   * @param archiveDir local archvie directory to copy into, e.g. /mnt/ftp/download/0000020-130108132303336
   * @param dataTable like download_tmp_1234
   * @param citationTable like download_tmp_citation_1234
   * @param hdfsPath like /user/hive/warehouse
   * @throws IOException on any read or write problems
   */
  @VisibleForTesting
  protected ArchiveBuilder(String downloadId, String user, String query,
    DatasetService datasetService, DatasetOccurrenceDownloadUsageService datasetUsageService,
    Configuration conf, FileSystem hdfs, FileSystem localfs, File archiveDir, String interpretedDataTable,
    String verbatimDataTable, String multimediaDataTable, String citationTable, String hdfsPath,
    String downloadLink, boolean isSmallDownload)
    throws MalformedURLException {
    this.downloadId = downloadId;
    this.user = user;
    this.query = query;
    this.datasetService = datasetService;
    this.datasetUsageService = datasetUsageService;
    this.conf = conf;
    this.hdfs = hdfs;
    this.localfs = localfs;
    this.archiveDir = archiveDir;
    this.interpretedDataTable = interpretedDataTable;
    this.verbatimDataTable = verbatimDataTable;
    this.multimediaDataTable = multimediaDataTable;
    this.citationTable = citationTable;
    this.hdfsPath = hdfsPath;
    this.dataset = new Dataset();
    this.downloadLink = new URL(downloadLink);
    this.isSmallDownload = isSmallDownload;
  }

  /**
   * Entry point for assembling the dwc archive.
   * The thrown exception is the only way of telling Oozie that this job has failed.
   * 
   * @throws IOException if any read/write operation failed
   */
  public static void main(String[] args) throws IOException {
    final String nameNode = args[0];      // same as namenode, like hdfs://c1n2.gbif.org:8020
    final String hdfsHivePath = args[1];  // path on hdfs to hive results
    final String interpretedDataTable = args[2];     // hive occurrence results table
    final String verbatimDataTable = args[3];     // hive occurrence results table
    final String multimediaDataTable = args[4];     // hive multimedia results table
    final String citationTable = args[5]; // hive citation results table
    final String downloadDir = args[6];   // locally mounted download dir
    // for example 0000020-130108132303336
    final String downloadId = DownloadUtils.workflowToDownloadId(args[7]);
    final String user = args[8];          // download user
    final String query = args[9];         // download query filter
    final String downloadLink = args[10];  // download link to the final zip archive
    final String registryWs = args[11];    // registry ws url
    final String isSmallDownload = args[12];    // isSmallDownload
    // download link needs to be constructed
    final String downloadLinkWithId = downloadLink.replace(DownloadUtils.DOWNLOAD_ID_PLACEHOLDER, downloadId);

    // create temporary, local, download specific directory
    File archiveDir = new File(downloadDir, downloadId);
    RegistryClientUtil registryClientUtil = new RegistryClientUtil();

    // create registry client
    DatasetService datasetService = registryClientUtil.setupDatasetService(registryWs);

    DatasetOccurrenceDownloadUsageService datasetUsageService = registryClientUtil.setupDatasetUsageService(registryWs);

    // filesystem configs
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, nameNode);
    FileSystem hdfs = FileSystem.get(conf);
    FileSystem localfs = FileSystem.getLocal(conf);

    // build archive
    ArchiveBuilder generator =
      new ArchiveBuilder(downloadId, user, query, datasetService, datasetUsageService, conf, hdfs, localfs, archiveDir,
        interpretedDataTable, verbatimDataTable, multimediaDataTable, citationTable, hdfsHivePath, downloadLinkWithId,
        Boolean.parseBoolean(isSmallDownload));
    LOG.info("ArchiveBuilder instance created with parameters:{}", Joiner.on(" ").skipNulls().join(args));
    generator.buildArchive(new File(downloadDir, downloadId + ".zip"));

    // SUCCESS!
  }

  /**
   * Main method to assemble the dwc archive and do all the work until we have a final zip file.
   * 
   * @param zipFile the final zip file holding the entire archive
   */
  public void buildArchive(File zipFile) throws DownloadException {
    LOG.info("Start building the archive {} ", zipFile.getPath());

    try {
      // oozie might try several times to run this job, so make sure our filesystem is clean
      cleanupFS();

      // create the temp archive dir
      archiveDir.mkdirs();

      // occurrence files interpreted and verbatim
      addOccurrenceDataFile(interpretedDataTable, HeadersFileUtil.DEFAULT_INTERPRETED_FILE_NAME, INTERPRETED_FILENAME);
      addOccurrenceDataFile(verbatimDataTable, HeadersFileUtil.DEFAULT_VERBATIM_FILE_NAME, VERBATIM_FILENAME);
      addOccurrenceDataFile(multimediaDataTable, HeadersFileUtil.DEFAULT_MULTIMEDIA_FILE_NAME, MULTIMEDIA_FILENAME);

      // metadata, citation and rights
      addDatasetMetadata();

      // metadata about the entire archive data
      addQueryMetadata();

      // meta.xml
      DwcArchiveUtils.createArchiveDescriptor(archiveDir);

      // zip up
      LOG.info("Zipping archive {}", archiveDir.toString());
      CompressionUtil.zipDir(archiveDir, zipFile, true);

    } catch (IOException e) {
      throw new DownloadException(e);

    } finally {
      // always cleanUp temp dir
      cleanupFS();
    }

  }

  public void createEmlFile(final UUID constituentId, final File emlDir) throws IOException {
    Closer closer = Closer.create();
    try {
      // store dataset EML as constituent metadata
      InputStream in = closer.register(datasetService.getMetadataDocument(constituentId));
      if (in != null) {
        // copy into archive, reading stream from registry services
        OutputStream out = closer.register(new FileOutputStream(new File(emlDir, constituentId + ".xml")));
        ByteStreams.copy(in, out);
      } else {
        LOG.error("Found no EML for datasetId {}", constituentId);
      }

    } catch (FileNotFoundException ex) {
      LOG.error("Error creating eml file", ex);
    } catch (IOException ex) {
      LOG.error("Error creating eml file", ex);
    } finally {
      closer.close();
    }
  }

  /**
   * Adds an eml file per dataset involved into a subfolder "dataset" which is supported by our dwc archive reader.
   * Create a rights.txt and citation.txt file targeted at humans to quickly yield an overview about rights and
   * datasets involved.
   * 
   * @throws IOException
   */
  private void addDatasetMetadata() throws IOException {
    LOG.info("Adding constituent dataset metadata to archive");
    Path citationSrc = new Path(hdfsPath + Path.SEPARATOR + citationTable);
    // now read the dataset citation table and create an EML file per datasetId
    // first copy from HDFS to local file
    if (!hdfs.exists(citationSrc)) {
      LOG.error("No citation file directory existing on HDFS, skip creating of dataset metadata {}", citationSrc);
      return;
    }

    final Map<UUID, Integer> srcDatasets = readDatasetCounts(citationSrc);

    File emlDir = new File(archiveDir, "dataset");
    if (!srcDatasets.isEmpty()) {
      emlDir.mkdir();
    }
    Closer closer = Closer.create();

    Writer rightsWriter = closer.register(FileUtils.startNewUtf8File(new File(archiveDir, RIGHTS_FILENAME)));
    Writer citationWriter = closer.register(FileUtils.startNewUtf8File(new File(archiveDir, CITATIONS_FILENAME)));

    closer.register(citationWriter);
    // write fixed citations header
    citationWriter.write(CITATION_HEADER);
    // now iterate over constituent UUIDs

    for (Entry<UUID, Integer> dsEntry : srcDatasets.entrySet()) {
      final UUID constituentId = dsEntry.getKey();
      LOG.info("Processing constituent dataset: {}", constituentId);
      // catch errors for each uuid to make sure one broken dataset does not bring down the entire process
      try {
        Dataset srcDataset = datasetService.get(constituentId);
        // contacts are not eagerly loaded
        srcDataset.setContacts(datasetService.listContacts(constituentId));

        // citation
        String citationLink = writeCitation(citationWriter, srcDataset, constituentId);
        // rights
        writeRights(rightsWriter, srcDataset, citationLink);
        // eml file
        createEmlFile(constituentId, emlDir);

        // add as constituent for later
        constituents.add(new Constituent(srcDataset.getTitle(), dsEntry.getValue()));

        // add original author as content provider to main dataset description
        Contact provider = getContentProviderContact(srcDataset);
        if (provider != null) {
          dataset.getContacts().add(provider);
        }
      } catch (UniformInterfaceException e) {
        LOG.error(String.format("Registry client http exception: %d \n %s", e.getResponse().getStatus(), e
          .getResponse().getEntity(String.class)), e);
      } catch (Exception e) {
        LOG.error("Error creating download file", e);
      }
    }
    closer.close();
  }

  /**
   * Copies and merges the hive query results files into a single, local occurrence data file.
   */
  private void addOccurrenceDataFile(String dataTable, String headerFileName, String destFileName) throws IOException {
    LOG.info("Copy-merge occurrence data hdfs file {} to local filesystem", dataTable);
    final Path dataSrc = new Path(hdfsPath + Path.SEPARATOR + dataTable);
    final Path headerFileDest = new Path(dataSrc + Path.SEPARATOR + HEADERS_FILENAME);
    if (!isSmallDownload) { // small downloads already include the headers
      FileUtil.copy(new File(headerFileName), hdfs, headerFileDest, false, conf);
    }
    File rawDataResult = new File(archiveDir, destFileName);
    Path dataDest = new Path(rawDataResult.toURI());
    FileUtil.copyMerge(hdfs, dataSrc, localfs, dataDest, false, conf, null);
    // remove the CRC file created by copyMerge method
    removeDataCRCFile(destFileName);
  }

  /**
   * Creates a single EML metadata file for the entire archive.
   * Make sure we execute this method AFTER building the constituents metadata which adds to our dataset instance.
   * 
   * @throws IOException
   */
  private void addQueryMetadata() {
    LOG.info("Add query dataset metadata to archive");
    try {
      // Random UUID use because the downloadId is not a string in UUID format
      dataset.setKey(UUID.randomUUID());
      dataset.setTitle(String.format(DATASET_TITLE_FMT, downloadId));
      dataset.setDescription(getDatasetDescription());
      dataset.setCreated(new Date());
      Citation citation = new Citation(String.format(DATASET_TITLE_FMT, downloadId), downloadId);
      dataset.setCitation(citation);
      // can we derive a link from the query to set the dataset.homepage?
      dataset.setPubDate(new Date());
      dataset.setDataLanguage(Language.ENGLISH);
      dataset.setType(DatasetType.OCCURRENCE);
      dataset.getDataDescriptions().add(createDataDescription());

      dataset.getContacts().add(createContact(user, null, ContactType.AUTHOR, true));
      dataset.getContacts().add(createContact(DOWNLOAD_CONTACT_SERVICE, null, ContactType.ORIGINATOR, true));
      dataset.getContacts().add(createContact(DOWNLOAD_CONTACT_SERVICE, null, ContactType.METADATA_AUTHOR, true));


      File eml = new File(archiveDir, METADATA_FILENAME);
      Writer writer = FileUtils.startNewUtf8File(eml);
      EMLWriter.write(dataset, writer);

    } catch (Exception e) {
      LOG.error("Failed to write query result dataset EML file", e);
    }
  }


  /**
   * Removes all temporary file system artifacts but the final zip archive.
   */
  private void cleanupFS() throws DownloadException {
    try {
      LOG.info("Cleaning up archive directory {}", archiveDir.getPath());
      localfs.delete(new Path(archiveDir.toURI()), true);
      archiveDir.delete();
    } catch (IOException e) {
      throw new DownloadException(e);
    }
  }

  /**
   * Utility method that creates a Contact with a limited number of fields.
   */
  private Contact createContact(String name, String email, ContactType type, boolean preferred) {
    Contact contact = new Contact();
    contact.setEmail(email);
    contact.setLastName(name);
    contact.setType(new InterpretedEnum<String, ContactType>(type.toString(), type).getInterpreted());
    return contact;
  }

  private DataDescription createDataDescription() {
    // link back to archive
    DataDescription dataDescription = new DataDescription();
    dataDescription.setFormat(DATA_DESC_FORMAT);
    dataDescription.setCharset(Charsets.UTF_8.displayName());
    try {
      dataDescription.setUrl(downloadLink.toURI());
    } catch (URISyntaxException e) {
      LOG.error(String.format("Wrong url %s", downloadLink), e);
    }
    return dataDescription;
  }

  /**
   * Checks the contacts of a dataset and finds the preferred contact that should be used as the main author
   * of a dataset.
   * 
   * @return preferred author contact or null
   */
  private Contact getContentProviderContact(Dataset dataset) {
    Contact author = null;
    for (ContactType type : AUTHOR_TYPES) {
      for (Contact c : dataset.getContacts()) {
        if (type == c.getType()) {
          if (author == null) {
            author = c;
          } else if (c.isPrimary()) {
            author = c;
          }
        }
      }
      if (author != null) {
        Contact provider = new Contact();
        try {
          PropertyUtils.copyProperties(provider, author);
          provider.setKey(null);
          provider.setType(ContactType.CONTENT_PROVIDER);
          provider.setPrimary(false);
          return provider;
        } catch (IllegalAccessException e) {
          LOG.error("Error setting provider contact", e);
        } catch (InvocationTargetException e) {
          LOG.error("Error setting provider contact", e);
        } catch (NoSuchMethodException e) {
          LOG.error("Error setting provider contact", e);
        }
      }
    }
    return null;
  }

  /**
   * Creates the dataset description.
   */
  private String getDatasetDescription() {
    StringBuilder description = new StringBuilder();
    // TODO: transform json filter into human readable query
    description.append(String.format(METADATA_DESC_HEADER_FMT, query));
    List<Constituent> byRecords = constituentsOrder.sortedCopy(constituents);
    for (Constituent c : byRecords) {
      description.append(c.records + " records from " + c.title + "<br/>");
    }
    return description.toString();
  }

  /**
   * Persists the dataset usage information and swallows any exception to avoid an error during the file building.
   */
  private void persistDatasetUsage(Integer count, String downloadKey, UUID datasetKey) {
    try {
      DatasetOccurrenceDownloadUsage datasetUsage = new DatasetOccurrenceDownloadUsage();
      datasetUsage.setDatasetKey(datasetKey);
      datasetUsage.setNumberRecords(count);
      datasetUsage.setDownloadKey(downloadKey);
      datasetUsageService.create(datasetUsage);
    } catch (Exception e) {
      LOG.error("Error persisting dataset usage information", e);
    }
  }

  /**
   * Creates Map with dataset UUIDs and its record counts.
   */
  private Map<UUID, Integer> readDatasetCounts(Path citationSrc) throws IOException {
    // the hive query result is a directory with one or more files - read them all into a uuid set
    Map<UUID, Integer> srcDatasets = Maps.newHashMap(); // map of uuids to occurrence counts
    FileStatus[] citFiles = hdfs.listStatus(citationSrc);
    int invalidUuids = 0;
    Closer closer = Closer.create();
    for (FileStatus fs : citFiles) {
      if (!fs.isDirectory()) {
        BufferedReader citationReader =
          new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath()), Charsets.UTF_8));
        closer.register(citationReader);
        try {
          String line = citationReader.readLine();
          while (line != null) {
            if (!Strings.isNullOrEmpty(line)) {
              // we also catch errors for every dataset so we dont break the loop
              try {
                Iterator<String> iter = TAB_SPLITTER.split(line).iterator();
                // play safe and make sure we got a uuid - even though our api doesnt require it
                UUID key = UUID.fromString(iter.next());
                Integer count = Integer.parseInt(iter.next());
                srcDatasets.put(key, count);
                // small downloads persist dataset usages while builds the citations file
                if (!this.isSmallDownload) {
                  persistDatasetUsage(count, downloadId, key);
                }
              } catch (IllegalArgumentException e) {
                // ignore invalid UUIDs
                LOG.info("Found invalid UUID as datasetId {}", line);
                invalidUuids++;
              }
            }
            line = citationReader.readLine();
          }
        } finally {
          closer.close();
        }
      }
    }
    if (invalidUuids > 0) {
      LOG.info("Found {} invalid dataset UUIDs", invalidUuids);
    } else {
      LOG.info("All {} dataset UUIDs are valid", srcDatasets.size());
    }
    return srcDatasets;
  }

  /**
   * Removes the file .occurrence.txt.crc that is created by the function FileUtil.copyMerge.
   * This method is temporary change to fix the issue http://dev.gbif.org/issues/browse/OCC-306.
   */
  private void removeDataCRCFile(String destFileName) {
    File occCRCDataFile = new File(archiveDir, String.format(CRC_FILE_FMT, destFileName));
    if (occCRCDataFile.exists()) {
      occCRCDataFile.delete();
    }
  }

  private String writeCitation(final Writer citationWriter, final Dataset dataset, final UUID constituentId)
    throws IOException {
    // citation
    String citationLink = null;
    if (dataset.getCitation() != null && !Strings.isNullOrEmpty(dataset.getCitation().getText())) {
      citationWriter.write('\n' + dataset.getCitation().getText());
      if (!Strings.isNullOrEmpty(dataset.getCitation().getIdentifier())) {
        citationLink = ", " + dataset.getCitation().getIdentifier();
        citationWriter.write(citationLink);
      }
    } else {
      LOG.error(String.format("Constituent dataset misses mandatory citation for id: %s", constituentId));
    }
    return citationLink;
  }

  /**
   * Write rights text.
   * 
   * @param rightsWriter
   * @param dataset
   * @param citationLink
   * @throws IOException
   */
  private void writeRights(final Writer rightsWriter, final Dataset dataset, final String citationLink)
    throws IOException {
    // write rights
    rightsWriter.write("\n\nDataset: " + dataset.getTitle());
    if (!Strings.isNullOrEmpty(citationLink)) {
      rightsWriter.write(citationLink);
    }
    rightsWriter.write("\nRights as supplied: ");
    if (!Strings.isNullOrEmpty(dataset.getRights())) {
      rightsWriter.write(dataset.getRights());
    } else {
      rightsWriter.write("Not supplied");
    }
  }
}
