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
package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ToLocalISO8601UDFTest {

  @Test
  public void testLocalDateFormatter() {
    ToLocalISO8601UDF function = new ToLocalISO8601UDF();

    assertNull(function.evaluate(new Text()));
    assertNull(function.evaluate(new Text("")));
    assertEquals("2020-04-02T16:48:54", function.evaluate(new Text("1585846134000")).toString());
    assertEquals("2020-04-02T16:48:54.001", function.evaluate(new Text("1585846134001")).toString());
  }

  @Test
  public void testUtcDateFormatter() {
    ToISO8601UDF function = new ToISO8601UDF();

    assertNull(function.evaluate(new Text()));
    assertNull(function.evaluate(new Text("")));
    assertEquals("2020-04-02T16:48:54Z", function.evaluate(new Text("1585846134000")).toString());
    assertEquals("2020-04-02T16:48:54.001Z", function.evaluate(new Text("1585846134001")).toString());
  }
}
