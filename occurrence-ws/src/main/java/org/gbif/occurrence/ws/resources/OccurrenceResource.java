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
package org.gbif.occurrence.ws.resources;


import org.gbif.api.annotation.NullToNotFound;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.persistence.experimental.OccurrenceRelationshipService;
import org.gbif.occurrence.search.OccurrenceGetByKey;
import org.gbif.occurrence.ws.provider.OccurrenceDwcXMLConverter;
import org.gbif.occurrence.ws.provider.OccurrenceVerbatimDwcXMLConverter;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import static org.gbif.ws.paths.OccurrencePaths.FRAGMENT_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.VERBATIM_PATH;

/**
 * Occurrence resource, the verbatim sub resource, and occurrence metrics.
 */
@RestController
@RequestMapping(
  value = OCCURRENCE_PATH,
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class OccurrenceResource {

  @VisibleForTesting
  public static final String ANNOSYS_PATH = "annosys";

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceResource.class);

  private final OccurrenceService occurrenceService;
  private final OccurrenceRelationshipService occurrenceRelationshipService;
  private final OccurrenceGetByKey occurrenceGetByKey;

  @Autowired
  public OccurrenceResource(
    OccurrenceService occurrenceService,
    OccurrenceGetByKey occurrenceGetByKey,
    OccurrenceRelationshipService occurrenceRelationshipService
  ) {
    this.occurrenceService = occurrenceService;
    this.occurrenceGetByKey = occurrenceGetByKey;
    this.occurrenceRelationshipService = occurrenceRelationshipService;
  }

  /**
   * This retrieves a single Occurrence detail by its key from the occurrence store.
   *
   * @param key Occurrence key
   * @return requested Occurrence or null if none could be found
   */
  @NullToNotFound
  @GetMapping("{id}")
  public Occurrence get(@PathVariable("id") Long key) {
    LOG.debug("Request Occurrence [{}]:", key);
    return occurrenceGetByKey.get(key);
  }

  /**
   * This retrieves a single occurrence fragment in its raw form as a string.
   *
   * @param key The Occurrence key
   * @return requested occurrence fragment or null if none could be found
   */
  @GetMapping("/{key}/" + FRAGMENT_PATH)
  @ResponseBody
  @NullToNotFound
  public String getFragment(@PathVariable("key") Long key) {
    LOG.debug("Request occurrence fragment [{}]:", key);
    return occurrenceService.getFragment(key);
  }

  /**
   * This retrieves a single occurrence fragment in its raw form as a string.
   *
   * @param datasetKey dataset UUID identifier
   * @param occurrenceId record identifier in the dataset
   * @return requested occurrence or null if none could be found
   */
  @GetMapping("/{datasetKey}/{occurrenceId}/" + FRAGMENT_PATH)
  @ResponseBody
  @NullToNotFound
  public String getFragment(@PathVariable("datasetKey") UUID datasetKey, @PathVariable("occurrenceId") String occurrenceId) {
    LOG.debug("Retrieve occurrence by dataset [{}] and occcurrenceId [{}]", datasetKey, occurrenceId);
    Occurrence occurrence = occurrenceGetByKey.get(datasetKey, occurrenceId);
    if (occurrence != null) {
      return getFragment(occurrence.getKey());
    }
    return null;
  }


  /**
   * This retrieves a single occurrence in its raw form as a string.
   *
   * @param datasetKey dataset UUID identifier
   * @param occurrenceId record identifier in the dataset
   *
   * @return requested occurrence or null if none could be found
   */
  @GetMapping("/{datasetKey}/{occurrenceId}")
  @ResponseBody
  @NullToNotFound
  public Occurrence get(@PathVariable("datasetKey") UUID datasetKey, @PathVariable("occurrenceId") String occurrenceId) {
    LOG.debug("Retrieve occurrence by dataset [{}] and occcurrenceId [{}]", datasetKey, occurrenceId);
    return occurrenceGetByKey.get(datasetKey, occurrenceId);
  }

  /**
   * This retrieves a single VerbatimOccurrence detail by its key from the occurrence store and transforms it into the API
   * version which uses Maps.
   *
   * @param key The Occurrence key
   * @return requested VerbatimOccurrence or null if none could be found
   */
  @GetMapping("/{key}/" + VERBATIM_PATH)
  @NullToNotFound
  public VerbatimOccurrence getVerbatim(@PathVariable("key") Long key) {
    LOG.debug("Request VerbatimOccurrence [{}]:", key);
    return occurrenceGetByKey.getVerbatim(key);
  }

  /**
   * This retrieves a single occurrence fragment in its raw form as a string.
   *
   * @param datasetKey dataset UUID identifier
   * @param occurrenceId record identifier in the dataset
   *
   * @return requested VerbatimOccurrence or null if none could be found
   */
  @GetMapping("/{datasetKey}/{occurrenceId}/" + VERBATIM_PATH)
  @ResponseBody
  @NullToNotFound
  public VerbatimOccurrence getVerbatim(@PathVariable("datasetKey") UUID datasetKey, @PathVariable("occurrenceId") String occurrenceId) {
    LOG.debug("Retrieve occurrence verbatim by dataset [{}] and occcurrenceId [{}]", datasetKey, occurrenceId);
    return occurrenceGetByKey.getVerbatim(datasetKey, occurrenceId);
  }

  /**
   * Provides a list of related occurrence records in JSON.
   * @return A list of related occurrences or an empty list if relatinships are not configured or none exist.
   */
  @GetMapping("/{key}/experimental/related")
  public String getRelatedOccurrences(@PathVariable("key") Long key) {
    LOG.debug("Request RelatedOccurrences [{}]:", key);
    List<String> relationshipsAsJsonSnippets = occurrenceRelationshipService.getRelatedOccurrences(key);
    String currentOccurrenceAsJson = occurrenceRelationshipService.getCurrentOccurrence(key);
    return String.format("{\"currentOccurrence\":%s,\"relatedOccurrences\":[%s]}",
      currentOccurrenceAsJson,
      String.join(",", relationshipsAsJsonSnippets));
  }

  /**
   * Removed API call, which supported a stream of featured occurrences on the old GBIF.org homepage.
   * @return An empty list.
   */
  @GetMapping("featured")
  @ResponseBody
  @Deprecated
  public List<Object> getFeaturedOccurrences() {
    LOG.warn("Featured occurrences have been removed.");
    return Lists.newArrayList();
  }

  /**
   * This method is implemented specifically to support Annosys and is not advertised or
   * documented in the public API.  <em>It may be removed at any time without notice</em>.
   *
   * @param key
   * @return
   */
  @GetMapping(
    value = ANNOSYS_PATH + "/{key}",
    produces = MediaType.APPLICATION_XML_VALUE
  )
  public String getAnnosysOccurrence(@PathVariable("key") Long key) {
    LOG.debug("Request Annosys occurrence [{}]:", key);
    return OccurrenceDwcXMLConverter.occurrenceXMLAsString(occurrenceGetByKey.get(key));
  }

  /**
   * This method is implemented specifically to support Annosys and is not advertised or
   * documented in the public API.  <em>It may be removed at any time without notice</em>.
   *
   * @param key
   * @return
   */
  @NullToNotFound
  @GetMapping(
    value = ANNOSYS_PATH + "/{key}/" + VERBATIM_PATH,
    produces = MediaType.APPLICATION_XML_VALUE
  )
  public String getAnnosysVerbatim(@PathVariable("key") Long key) {
    LOG.debug("Request Annosys verbatim occurrence [{}]:", key);
    return OccurrenceVerbatimDwcXMLConverter.verbatimOccurrenceXMLAsString(occurrenceGetByKey.getVerbatim(key));
  }

}
