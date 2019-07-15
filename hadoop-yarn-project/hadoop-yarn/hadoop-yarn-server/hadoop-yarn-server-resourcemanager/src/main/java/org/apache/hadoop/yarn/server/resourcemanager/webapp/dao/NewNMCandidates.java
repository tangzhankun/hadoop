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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@XmlRootElement(name = "NewNMCandidates")
@XmlAccessorType(XmlAccessType.FIELD)
public class NewNMCandidates {

  public String getTip() {
    return tip;
  }

  public void setTip(String tip) {
    this.tip = tip;
  }

  public double getCostPerHour() {
    return costPerHour;
  }

  protected double costPerHour;
  protected String tip;

  public String getRecommendActionTime() {
    return recommendActionTime;
  }

  protected String recommendActionTime = "Now";

  protected ArrayList<NewSingleNMCandidate> getNewNMCandidates() {
    return newNMCandidates;
  }

  protected ArrayList<NewSingleNMCandidate> newNMCandidates =
      new ArrayList<>();

  public NewNMCandidates() {}

  public NewNMCandidates(ArrayList<NewSingleNMCandidate> m) {
    this.newNMCandidates = m;
  }

  public void add(NodeInstanceType type, int count) {
    tip = tip + "Before add method: (" + type;
    tip = tip + ", count:" + count + ")";
    tip = tip + ", costPerHour:" + costPerHour;
    if (newNMCandidates == null) {
      newNMCandidates = new ArrayList<>();
    }
    NewSingleNMCandidate e = null;
    for (int i = 0; i < newNMCandidates.size(); i++) {
      e = newNMCandidates.get(i);
      if (type.modelName.equals(e.modelName)) {
        break;
      }
    }
    if (e == null) {
      NewSingleNMCandidate newNM = new NewSingleNMCandidate(type.modelName, count);
      newNMCandidates.add(newNM);
    } else {
      e.count = e.count + count;
    }
    costPerHour += type.costPerHour * count;
    tip = tip + ", After add method: (" + type + ")";
    tip = tip + ". costPerHour:" + costPerHour;
    tip = tip + "\n\r";
  }

}
