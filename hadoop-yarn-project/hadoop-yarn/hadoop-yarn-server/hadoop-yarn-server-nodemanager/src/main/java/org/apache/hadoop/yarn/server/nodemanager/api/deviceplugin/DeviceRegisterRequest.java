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

import java.util.Objects;

/**
 * A vendor device plugin use this object to register
 * to NM device plugin framework
 * */
public class DeviceRegisterRequest {

  private final String version;
  private final String resourceName;

  public DeviceRegisterRequest(Builder builder) {
    this.version = Objects.requireNonNull(builder.version);
    this.resourceName = Objects.requireNonNull(builder.resourceName);
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getVersion() {
    return version;
  }

  public static class Builder {
    private String version;
    private String resourceName;

    private Builder() {}

    public static Builder newInstance() {
      return new Builder();
    }

    public DeviceRegisterRequest build() {
      return new DeviceRegisterRequest(this);
    }

    // TODO: add sematic versioning pattern check
    public Builder setVersion(String version) {
      this.version = version;
      return this;
    }

    public Builder setResourceName(String resourceName) {
      this.resourceName = resourceName;
      return this;
    }

  }
}
