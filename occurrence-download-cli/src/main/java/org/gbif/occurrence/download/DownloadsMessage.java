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

import org.gbif.common.messaging.api.Message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;


// TODO Move to postal service
@Data
public class DownloadsMessage implements Message {

  private final String jobId;

  private final String command;

  private final String query;

  @JsonCreator
  public DownloadsMessage(@JsonProperty(value = "jobId", required = true) String jobId, @JsonProperty(value = "command", required = true) String command, @JsonProperty(value = "query", required = false) String query) {
    this.jobId = jobId;
    this.command = command;
    this.query = query;
  }

  @Override
  public String getRoutingKey() {
    return "occurrence.download";
  }
}
