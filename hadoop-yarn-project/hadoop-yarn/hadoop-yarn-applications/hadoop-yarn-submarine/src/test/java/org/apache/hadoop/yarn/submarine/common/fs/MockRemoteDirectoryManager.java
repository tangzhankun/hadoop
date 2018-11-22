/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.common.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class MockRemoteDirectoryManager implements RemoteDirectoryManager {
  private File jobsParentDir = null;
  private File modelParentDir = null;

  private File jobDir = null;
  @Override
  public Path getJobStagingArea(String jobName, boolean create)
      throws IOException {
    if (jobsParentDir == null && create) {
      jobsParentDir = new File(
          "target/_staging_area_" + System.currentTimeMillis());
      if (!jobsParentDir.mkdirs()) {
        throw new IOException(
            "Failed to mkdirs for" + jobsParentDir.getAbsolutePath());
      }
    }

    this.jobDir = new File(jobsParentDir.getAbsolutePath(), jobName);
    if (create && !jobDir.exists()) {
      if (!jobDir.mkdirs()) {
        throw new IOException("Failed to mkdirs for "
            + jobDir.getAbsolutePath());
      }
    }
    return new Path(jobDir.getAbsolutePath());
  }

  @Override
  public Path getJobCheckpointDir(String jobName, boolean create)
      throws IOException {
    return new Path("s3://generated_checkpoint_dir");
  }

  @Override
  public Path getModelDir(String modelName, boolean create)
      throws IOException {
    if (modelParentDir == null && create) {
      modelParentDir = new File(
          "target/_models_" + System.currentTimeMillis());
      if (!modelParentDir.mkdirs()) {
        throw new IOException(
            "Failed to mkdirs for " + modelParentDir.getAbsolutePath());
      }
    }

    File modelDir = new File(modelParentDir.getAbsolutePath(), modelName);
    if (create) {
      if (!modelDir.exists() && !modelDir.mkdirs()) {
        throw new IOException("Failed to mkdirs for "
            + modelDir.getAbsolutePath());
      }
    }
    return new Path(modelDir.getAbsolutePath());
  }

  @Override
  public FileSystem getFileSystem() throws IOException {
    return FileSystem.getLocal(new Configuration());
  }

  @Override
  public Path getUserRootFolder() throws IOException {
    return new Path("s3://generated_root_dir");
  }

  @Override
  public boolean isDir(String uri) throws IOException {
    return getFileSystem().getFileStatus(
        new Path(convertToStagingPath(uri))).isDirectory();

  }

  /**
   * We use staging dir as mock HDFS dir
   * */
  @Override
  public boolean copyFilesFromHdfs(String remoteDir, String localDir)
      throws IOException {
    // mock the copy from HDFS into a local copy
    Path remoteToLocalDir = new Path(convertToStagingPath(remoteDir));
    File old = new File(convertToStagingPath(localDir));
    if (old.isDirectory() && old.exists()) {
      if (!FileUtil.fullyDelete(old)) {
        throw new IOException("Cannot delete temp dir:"
            + old.getAbsolutePath());
      }
    }
    return FileUtil.copy(getFileSystem(), remoteToLocalDir,
        new File(localDir), false,
        getFileSystem().getConf());
  }

  private String convertToStagingPath(String uri) {
    String ret = uri;
    if (uri.startsWith("hdfs://")) {
      String dirName = new Path(uri).getName();
      ret = this.jobDir.getAbsolutePath()
          + "/" + dirName;
    }
    return ret;
  }

  @Override
  public boolean existsHdfsFile(Path uri) throws IOException {
    String fakeLocalFilePath = this.jobDir.getAbsolutePath()
        + "/" + uri.getName();
    return new File(fakeLocalFilePath).exists();
  }

  @Override
  public FileStatus getHdfsFileStatus(Path p) throws IOException {
    return getFileSystem().getFileStatus(new Path(
        convertToStagingPath(p.toUri().toString())));
  }

}
