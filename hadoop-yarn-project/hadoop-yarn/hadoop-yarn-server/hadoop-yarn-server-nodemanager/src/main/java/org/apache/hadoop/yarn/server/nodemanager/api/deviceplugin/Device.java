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

public class Device implements Serializable, Comparable {

  private static final long serialVersionUID = 1L;

  /**
   * Required fields:
   * ID: an plugin specified index number
   * devPath: device file like /dev/devicename
   * majorNumber: major device number
   * minorNumber: minor device number
   * busID: PCI Bus ID in format [[[[<domain>]:]<bus>]:][<slot>][.[<func>]]. Can get from "lspci -D"
   * isHealthy: true or false indicating device health status
   * */
  private final Integer ID;
  private final String devPath;
  private final Integer majorNumber;
  private final Integer minorNumber;
  private final String busID;
  private boolean isHealthy;

  /**
   * Optional fields
   * */
  private String status;
  // TODO: topology and attributes

  private Device(Builder builder) {
    this.ID = Objects.requireNonNull(builder.ID);
    this.devPath = Objects.requireNonNull(builder.devPath);
    this.majorNumber = Objects.requireNonNull(builder.majorNumber);
    this.minorNumber = Objects.requireNonNull(builder.minorNumber);
    this.busID = Objects.requireNonNull(builder.busID);
    this.isHealthy = Objects.requireNonNull(builder.isHealthy);
  }

  public Integer getID() {
    return ID;
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
    if (o == null || getClass() != o.getClass()){
      return false;
    }
    Device device = (Device) o;
    return Objects.equals(ID, device.ID) &&
        Objects.equals(devPath, device.devPath) &&
        Objects.equals(majorNumber, device.majorNumber) &&
        Objects.equals(minorNumber, device.minorNumber) &&
        Objects.equals(busID, device.busID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ID, devPath, majorNumber, minorNumber, busID);
  }

  @Override
  public int compareTo(Object o) {
    if (o == null || (!(o instanceof Device))) {
      return -1;
    }

    Device other = (Device) o;

    int result = Integer.compare(ID, other.getID());
    if (0 != result) {
      return result;
    }

    result = Integer.compare(majorNumber, other.majorNumber);
    if (0 != result) {
      return result;
    }

    result = Integer.compare(minorNumber, other.minorNumber);
    if (0 != result) {
      return result;
    }

    result = devPath.compareTo(other.devPath);
    if (0 != result) {
      return result;
    }

    return busID.compareTo(other.busID);
  }

  @Override
  public String toString() {
    return "(" + getDevPath() + ", " + getID() + ", " + getMajorNumber() + ":" + getMinorNumber() + ")";
  }

  public static class Builder {
    private Integer ID;
    private String devPath;
    private Integer majorNumber;
    private Integer minorNumber;
    private String busID;
    private boolean isHealthy;
    private String status;

    private Builder() {}

    public static Builder newInstance() {
      return new Builder();
    }

    public Device build(){
      return new Device(this);
    }

    public Builder setID(Integer ID) {
      this.ID = ID;
      return this;
    }

    public Builder setDevPath(String devPath) {
      this.devPath = devPath;
      return this;
    }

    public Builder setMajorNumber(Integer majorNumber) {
      this.majorNumber = majorNumber;
      return this;
    }

    public Builder setMinorNumber(Integer minorNumber) {
      this.minorNumber = minorNumber;
      return this;
    }

    public Builder setBusID(String busID) {
      this.busID = busID;
      return this;
    }

    public Builder setHealthy(boolean healthy) {
      isHealthy = healthy;
      return this;
    }

    public Builder setStatus(String status) {
      this.status = status;
      return this;
    }

  }
}
