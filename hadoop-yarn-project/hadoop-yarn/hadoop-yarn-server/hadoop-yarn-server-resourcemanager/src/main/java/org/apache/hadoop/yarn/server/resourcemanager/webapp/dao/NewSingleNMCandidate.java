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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "NewSingleNMCandidate")
@XmlAccessorType(XmlAccessType.FIELD)
public class NewSingleNMCandidate {
  public String getModelName() {
    return modelName;
  }

  public double getCostPerHour() {
    return costPerHour;
  }

  public void setCostPerHour(double costPerHour) {
    this.costPerHour = costPerHour;
  }

  protected double costPerHour;

  public int getCount() {
    return count;
  }

  protected String modelName;
  protected int count;
  public NewSingleNMCandidate() { }

  public NewSingleNMCandidate(String n, int c, double d) {
    this.modelName = n;
    this.count = c;
    this.costPerHour = d;
  }
}
