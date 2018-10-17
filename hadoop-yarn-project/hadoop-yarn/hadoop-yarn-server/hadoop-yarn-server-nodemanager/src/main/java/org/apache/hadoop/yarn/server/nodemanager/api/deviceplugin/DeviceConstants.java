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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

public class DeviceConstants {
  /**
   * Follows the "majorVersion.minorVersion.patchVersion" convention
   * Given a version number MAJOR.MINOR.PATCH, increment the:
   *
   * MAJOR version when you make incompatible API changes,
   * MINOR version when you add functionality in a backwards-compatible manner, and
   * PATCH version when you make backwards-compatible bug fixes.
   * */
  public static final String version = "0.1.0";
}
