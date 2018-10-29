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

package org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin;

import java.io.Serializable;
import java.util.Objects;

public class DeviceLink implements Serializable, Comparable {

  private static final long serialVersionUID = 1L;

  String busId;
  DeviceLinkType linkType;

  public String getBusId() {
    return busId;
  }

  public DeviceLinkType getLinkType() {
    return linkType;
  }

  private DeviceLink(Builder builder) {
    this.busId = builder.busId;
    this.linkType = builder.linkType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()){
      return false;
    }
    DeviceLink other = (DeviceLink) o;
    return Objects.equals(other.getBusId(), busId) &&
        Objects.equals(other.getLinkType(), linkType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(busId, linkType);
  }

  @Override
  public int compareTo(Object o) {
    if (o == null || (!(o instanceof DeviceLink))) {
      return -1;
    }
    DeviceLink other = (DeviceLink) o;

    return other.linkType.compareTo(getLinkType());
  }

  public static class Builder {
    String busId;
    DeviceLinkType linkType;

    private Builder(){}
    public static Builder newInstance() {
      return new Builder();
    }

    public DeviceLink build() {
      return new DeviceLink(this);
    }

    public Builder setBusId(String busId) {
      this.busId = busId;
      return this;
    }

    public Builder setLinkType(DeviceLinkType linkType) {
      this.linkType = linkType;
      return this;
    }
  }
}
