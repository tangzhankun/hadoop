/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli.param;

import org.apache.commons.cli.ParseException;

public class Localization {

  private String hdfsPathPattern = "^hdfs://(/[^/ ]*)+/?$";
  private String filePathPattern = "^(/[^/ ]*)+/?$";
  private String mountPattern = "(wr|rw|ro)$";
  private String remoteUri;
  private String localPath;

  public void parse(String arg) throws ParseException {
    if (arg.startsWith("hdfs://")) {
      int index = arg.in
    }

    String[] tokens = arg.split(":");
    if (tokens.length > 2) {
      throw new ParseException("Should be \"remoteUri:localFileName\" format for localization");
    }
    remoteUri = tokens[0].trim();
    localPath = tokens[1].trim();
  }

  public String getRemoteUri() {
    return remoteUri;
  }

  public void setRemoteUri(String remoteUri) {
    this.remoteUri = remoteUri;
  }

  public String getLocalPath() {
    return localPath;
  }

  public void setLocalPath(String localPath) {
    this.localPath = localPath;
  }

}
