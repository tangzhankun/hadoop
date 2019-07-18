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


import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.Resources;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.text.DecimalFormat;
import java.util.ArrayList;

@XmlRootElement(name = "NewNMCandidates")
@XmlAccessorType(XmlAccessType.NONE)
public class NewNMCandidates {

  public String getTip() {
    return tip;
  }

  public void setTip(String tip) {
    this.tip = tip;
  }

  public String getTotalCostPerHour() {
    return totalCostPerHour;
  }

  @XmlElement
  protected String totalCostPerHour;

  protected String tip;

  public String getRecommendActionTime() {
    return recommendActionTime;
  }

  @XmlElement
  protected String recommendActionTime = "Now";

  protected ArrayList<NewSingleTypeNMCandidate> getNewNMCandidates() {
    return newNMCandidates;
  }

  @XmlElement
  protected ArrayList<NewSingleTypeNMCandidate> newNMCandidates =
      new ArrayList<>();

  public NewNMCandidates() {}

  public NewNMCandidates(ArrayList<NewSingleTypeNMCandidate> m) {
    this.newNMCandidates = m;
  }

  public void add(NodeInstanceType type, int instanceCount,
      Resource planToUseInThisNodeType) {
    if (newNMCandidates == null) {
      newNMCandidates = new ArrayList<>();
    }
    NewSingleTypeNMCandidate e = null;
    for (int i = 0; i < newNMCandidates.size(); i++) {
      if (type.modelName.equals(newNMCandidates.get(i).modelName)) {
        e = newNMCandidates.get(i);
        break;
      }
    }
    if (e == null) {
      NewSingleTypeNMCandidate newNM = new NewSingleTypeNMCandidate(type.modelName,
          instanceCount, type.costPerHour,
          new CustomResourceInfo(
              Resources.multiplyAndRoundUp(type.getCapacity().getResource(),instanceCount)));
      newNM.setPlanToUse(new CustomResourceInfo(planToUseInThisNodeType));
      newNMCandidates.add(newNM);
    } else {
      e.addPlanToUse(planToUseInThisNodeType);
      e.count = e.count + instanceCount;
    }
    totalCostPerHour = calculateCost();
  }

  private String calculateCost() {
    double r = 0;
    for (int i = 0; i < newNMCandidates.size(); i++) {
      r += newNMCandidates.get(i).getCostPerHour() * newNMCandidates.get(i).getCount();
    }
    return new DecimalFormat("#.##").format(r);
  }

}
