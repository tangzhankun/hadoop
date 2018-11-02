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
import java.util.Set;
import java.util.TreeSet;

public class Device implements Serializable, Comparable {

  private static final long serialVersionUID = 1L;

  /**
   * Required fields:
   * ID: an plugin specified index number
   * majorNumber: major device number
   * minorNumber: minor device number
   * isHealthy: true or false indicating device health status
   *
   * Optional fields:
   * devPath: device file like /dev/devicename
   * busID: PCI Bus ID in format [[[[<domain>]:]<bus>]:][<slot>][.[<func>]]. Can get from "lspci -D"
   * topology: describe connection to other devices
   * Status: For future use
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

  /**
   * A {@code Device} has a topology which
   * represent the links to other {@code Device}s
   * */
  private final Set<DeviceLink> topology;

  private Device(Builder builder) {
    this.ID = Objects.requireNonNull(builder.ID);
    this.devPath = builder.devPath;
    this.majorNumber = Objects.requireNonNull(builder.majorNumber);
    this.minorNumber = Objects.requireNonNull(builder.minorNumber);
    this.busID = builder.busID;
    this.isHealthy = Objects.requireNonNull(builder.isHealthy);
    this.topology = builder.topology;
    this.status = builder.status;
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

  public Set<DeviceLink> getTopology() {
    return topology;
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
    return Objects.equals(ID, device.getID()) &&
        Objects.equals(devPath, device.getDevPath()) &&
        Objects.equals(majorNumber, device.getMajorNumber()) &&
        Objects.equals(minorNumber, device.getMinorNumber()) &&
        Objects.equals(busID, device.getBusID());
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
    return "(" + getID() + ", " + getDevPath() + ", " + getMajorNumber() + ":" + getMinorNumber() + ")";
  }

  public static class Builder {
    private Integer ID;
    private String devPath = "";
    private Integer majorNumber;
    private Integer minorNumber;
    private String busID = "";
    private boolean isHealthy;
    private String status = "";
    private Set<DeviceLink> topology;

    private Builder() {
      topology = new TreeSet<>();
    }

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

    public Builder addDeviceLink(DeviceLink link) {
      this.topology.add(link);
      return this;
    }

  }
}
