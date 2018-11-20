/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.common.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public interface RemoteDirectoryManager {
  Path getJobStagingArea(String jobName, boolean create) throws IOException;

  Path getJobCheckpointDir(String jobName, boolean create) throws IOException;

  Path getModelDir(String modelName, boolean create) throws IOException;

  FileSystem getFileSystem() throws IOException;

  Path getUserRootFolder() throws IOException;

  boolean isDir(String url) throws IOException;

  boolean copyFilesFromHdfs(String remoteDir, String localDir)
      throws IOException;
  boolean existsHdfsFile(Path url) throws IOException;

  FileStatus getHdfsFileStatus(Path url) throws IOException;
}