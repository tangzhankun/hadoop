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

  private Integer ID;
  private String devPath;
  private Integer majorNumber;
  private Integer minorNumber;
  private String busID;
  private boolean isHealthy;
  private String status;
  // TODO: topology and attributes

  public Device(Integer ID, String devPath, Integer majorNumber,
      Integer minorNumber, String busID, boolean isHealthy) {
    this.ID = ID;
    this.devPath = devPath;
    this.majorNumber = majorNumber;
    this.minorNumber = minorNumber;
    this.busID = busID;
    this.isHealthy = isHealthy;
  }

  public Integer getID() {
    return ID;
  }

  public void setID(Integer ID) {
    this.ID = ID;
  }

  public String getDevPath() {
    return devPath;
  }

  public void setDevPath(String devPath) {
    this.devPath = devPath;
  }

  public Integer getMajorNumber() {
    return majorNumber;
  }

  public void setMajorNumber(Integer majorNumber) {
    this.majorNumber = majorNumber;
  }

  public Integer getMinorNumber() {
    return minorNumber;
  }

  public void setMinorNumber(Integer minorNumber) {
    this.minorNumber = minorNumber;
  }

  public String getBusID() {
    return busID;
  }

  public void setBusID(String busID) {
    this.busID = busID;
  }

  public boolean isHealthy() {
    return isHealthy;
  }

  public void setHealthy(boolean healthy) {
    isHealthy = healthy;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
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
    return Integer.compare(minorNumber, other.minorNumber);
  }

  @Override
  public String toString() {
    return "(" + getDevPath() + ", " + getID() + ", " + getMajorNumber() + ":" + getMinorNumber() + ")";
  }
}
