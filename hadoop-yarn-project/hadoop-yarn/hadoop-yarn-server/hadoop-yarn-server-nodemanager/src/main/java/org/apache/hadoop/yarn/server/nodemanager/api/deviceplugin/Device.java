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

import java.io.Serializable;
import java.util.Objects;

/**
 * Represent one "device" resource.
 * */
public final class Device implements Serializable, Comparable {

  private static final long serialVersionUID = 1L;

  /**
   * An plugin specified index number.
   * Must set. Recommend starting from 0
   * */
  private final Integer id;

  /**
   * The device node like "/dev/devname".
   * Optional
   * */
  private final String devPath;

  /**
   * The major device number.
   * Optional
   * */
  private final Integer majorNumber;

  /**
   * The minor device number.
   * Optional
   * */
  private final Integer minorNumber;

  /**
   * PCI Bus ID in format [[[[<domain>]:]<bus>]:][<slot>][.[<func>]].
   * Optional. Can get from "lspci -D" in Linux
   * */
  private final String busID;

  /**
   * Is healthy or not.
   * false by default
   * */
  private boolean isHealthy;

  /**
   * Plugin customized status info.
   * Optional
   * */
  private String status;

  /**
   * Private constructor.
   * @param builder
   */
  private Device(Builder builder) {
    this.id = Objects.requireNonNull(builder.id);
    this.devPath = builder.devPath;
    this.majorNumber = builder.majorNumber;
    this.minorNumber = builder.minorNumber;
    this.busID = builder.busID;
    this.isHealthy = Objects.requireNonNull(builder.isHealthy);
    this.status = builder.status;
  }

  public Integer getId() {
    return id;
  }

  public String getDevPath() {
    return devPath;
  }

  public Integer getMajorNumber() {
    return majorNumber;
  }

  public Integer getMinorNumber() {
    return minorNumber;
  }

  public String getBusID() {
    return busID;
  }

  public boolean isHealthy() {
    return isHealthy;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Device device = (Device) o;
    return Objects.equals(id, device.getId())
        && Objects.equals(devPath, device.getDevPath())
        && Objects.equals(majorNumber, device.getMajorNumber())
        && Objects.equals(minorNumber, device.getMinorNumber())
        && Objects.equals(busID, device.getBusID());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, devPath, majorNumber, minorNumber, busID);
  }

  @Override
  public int compareTo(Object o) {
    if (o == null || (!(o instanceof Device))) {
      return -1;
    }

    Device other = (Device) o;

    int result = Integer.compare(id, other.getId());
    if (0 != result) {
      return result;
    }

    result = Integer.compare(majorNumber, other.getMajorNumber());
    if (0 != result) {
      return result;
    }

    result = Integer.compare(minorNumber, other.getMinorNumber());
    if (0 != result) {
      return result;
    }

    result = devPath.compareTo(other.getDevPath());
    if (0 != result) {
      return result;
    }

    return busID.compareTo(other.getBusID());
  }

  @Override
  public String toString() {
    return "(" + getId() + ", " + getDevPath() + ", "
        + getMajorNumber() + ":" + getMinorNumber() + ")";
  }

  public final static class Builder {
    private Integer id;
    private String devPath = "";
    private Integer majorNumber;
    private Integer minorNumber;
    private String busID = "";
    private boolean isHealthy;
    private String status = "";

    public static Builder newInstance() {
      return new Builder();
    }

    public Device build() {
      return new Device(this);
    }

    public Builder setId(Integer i) {
      this.id = i;
      return this;
    }

    public Builder setDevPath(String dp) {
      this.devPath = dp;
      return this;
    }

    public Builder setMajorNumber(Integer maN) {
      this.majorNumber = maN;
      return this;
    }

    public Builder setMinorNumber(Integer miN) {
      this.minorNumber = miN;
      return this;
    }

    public Builder setBusID(String bI) {
      this.busID = bI;
      return this;
    }

    public Builder setHealthy(boolean healthy) {
      isHealthy = healthy;
      return this;
    }

    public Builder setStatus(String s) {
      this.status = s;
      return this;
    }

  }
}
