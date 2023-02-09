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
package org.gbif.occurrence.ws.config;

import io.swagger.v3.oas.models.tags.Tag;
import org.springdoc.core.customizers.OpenApiCustomiser;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Java configuration of the OpenAPI specification.
 */
@Component
public class OpenAPIConfiguration {

  /**
   * Sorts tags (sections of the registry documentation) by the order extension, rather than alphabetically.
   */
  @Bean
  public OpenApiCustomiser sortTagsByOrderExtension() {
    return openApi -> openApi.setTags(openApi.getTags()
      .stream()
      .sorted(tagOrder())
      .collect(Collectors.toList()));
  }

  Comparator<Tag> tagOrder() {
    return Comparator.comparing(tag ->
      tag.getExtensions() == null ?
        "__" + tag.getName() :
        ((Map)tag.getExtensions().get("x-Order")).get("Order").toString());
  }
}
