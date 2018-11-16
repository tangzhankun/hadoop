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

  private String linuxFilePathPattern = "^(/[^/ ]*)+/?$";
  private String fsPathPattern = "(^hdfs://|.*)(/[^/ ]*)+/?$";
  private String mountPattern = "(wr|rw|ro)$";
  private String remoteUri;
  private String localPath;
  // Read only by default
  private String mountPermission = "ro";

  public void parse(String arg) throws ParseException {
    String[] tokens = arg.split(":");
    int minimumParts = 2;
    if (tokens[0].equals("hdfs")) {
      minimumParts = 3;
    }
    if (tokens.length < minimumParts || tokens.length > 4) {
      throw new ParseException("Invalid parameter,"
          + "Should be \"remoteUri:localPath:[ro|rw|wr]\" "
          + "format for --localizations");
    }

    /**
     * RemoteUri starts with hdfs://.
     * Merge part 0 and 1 to build a hdfs path in token[0].
     * toke[1] will be localPath
     * */
    if (minimumParts == 3) {
      tokens[0] = tokens[0] + tokens[1];
      tokens[1] = tokens[2];
      if (tokens.length == 4) {
        // Has permission part
        mountPermission = tokens[3];
      }
    }
    // RemoteUri starts with linux file path
    if (minimumParts == 2 && tokens.length == 3) {
      // Has permission part
      mountPermission = tokens[2];
    }
    remoteUri = tokens[0];
    if (!remoteUri.matches(fsPathPattern)) {
      throw new ParseException("Invalid remoteUri," +
          remoteUri);
    }
    localPath = tokens[1];
    if (!localPath.matches(linuxFilePathPattern)) {
      throw new ParseException("Invalid localPath," +
          localPath);
    }
    if (!mountPermission.matches(mountPattern)) {
      throw new ParseException("Invalid mount permission," +
          mountPermission);
    }
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
