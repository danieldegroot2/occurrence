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
package org.gbif.occurrence.download.file.archive;

import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.utils.file.FileUtils;
import org.gbif.utils.file.InputStreamUtils;

import java.net.URI;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiFileArchiveBuilderTest {

  @Test
  public void testBuildDefaultMode() throws Exception {
    FileSystem sourceFileSystem = new LocalFileSystem();
    sourceFileSystem.initialize(URI.create("file:///"), new Configuration());

    String[] arguments = {
      FileUtils.getClasspathFile("multitsv/default/first").getAbsolutePath(),
      "first.csv",
      "col1,col2,col3",

      FileUtils.getClasspathFile("multitsv/default/second").getAbsolutePath(),
      "second.csv",
      "",

      FileUtils.getClasspathFile("multitsv/default/third").getAbsolutePath(),
      "third.csv",
      "colⅠ,colⅡ,colⅢ",

      FileUtils.createTempDir().getAbsolutePath(),
      "empty.csv",
      "col一,col二,col三"
    };
    String targetPath = Files.createTempDirectory("multifile-default").toString();
    String downloadKey = "testArchive";

    MultiFileArchiveBuilder.withEntries(arguments)
      .mergeAllToZip(sourceFileSystem, sourceFileSystem, targetPath, downloadKey,
        ModalZipOutputStream.MODE.DEFAULT);

    ZipFile zf = new ZipFile(targetPath + "/testArchive.zip");
    ZipEntry ze = zf.getEntry("third.csv");
    assertEquals("colⅠ,colⅡ,colⅢ\na,b,c\nг,д,е\nη,θ,ι\n", new InputStreamUtils().readEntireStream(zf.getInputStream(ze)));
  }

  @Test
  public void testBuildPreDeflatedMode() throws Exception {
    FileSystem sourceFileSystem = new LocalFileSystem();
    sourceFileSystem.initialize(URI.create("file:///"), new Configuration());

    String[] arguments = {
      FileUtils.getClasspathFile("multitsv/pre_deflated/first").getAbsolutePath(),
      "first.csv",
      "col1,col2,col3",

      FileUtils.getClasspathFile("multitsv/pre_deflated/second").getAbsolutePath(),
      "second.csv",
      "",

      FileUtils.getClasspathFile("multitsv/pre_deflated/third").getAbsolutePath(),
      "third.csv",
      "colⅠ,colⅡ,colⅢ",

      FileUtils.createTempDir().getAbsolutePath(),
      "empty.csv",
      "col一,col二,col三"
    };
    String targetPath = Files.createTempDirectory("multifile-predeflate").toString();
    String downloadKey = "testArchive";

    MultiFileArchiveBuilder.withEntries(arguments)
      .mergeAllToZip(sourceFileSystem, sourceFileSystem, targetPath, downloadKey,
        ModalZipOutputStream.MODE.PRE_DEFLATED);

    ZipFile zf = new ZipFile(targetPath + "/testArchive.zip");
    ZipEntry ze = zf.getEntry("third.csv");
    assertEquals("colⅠ,colⅡ,colⅢ\na,b,c\nг,д,е\nη,θ,ι\n", new InputStreamUtils().readEntireStream(zf.getInputStream(ze)));
  }
}
