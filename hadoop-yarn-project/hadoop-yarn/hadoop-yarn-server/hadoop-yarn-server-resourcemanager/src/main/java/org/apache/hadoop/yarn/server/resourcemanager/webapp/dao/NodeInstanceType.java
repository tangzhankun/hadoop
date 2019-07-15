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
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "NodeInstanceType")
@XmlAccessorType(XmlAccessType.FIELD)
public class NodeInstanceType {

  protected enum ModelType {
    ONDEMAND,
    SPOT
  }

  public String getModelName() {
    return modelName;
  }

  public CustomResourceInfo getCapacity() {
    return capacity;
  }

  public ModelType getType() {
    return type;
  }

  public double getCostPerHour() {
    return costPerHour;
  }

  protected String modelName;
  protected CustomResourceInfo capacity;
  protected ModelType type;
  protected double costPerHour;

  public NodeInstanceType() {}

  public NodeInstanceType(String name, Resource res, ModelType t, double c) {
    this.capacity = new CustomResourceInfo(res);
    this.type = t;
    this.costPerHour = c;
    this.modelName = name;
  }

  public static NodeInstanceType[] getAllNodeInstanceType() {
    NodeInstanceType[] types = new NodeInstanceType[2];
    Resource r1 = Resource.newInstance(64 * 1024, 4);
    types[0] = new NodeInstanceType("p2.xlarge", r1, ModelType.ONDEMAND, 3.06);

    Resource r2 = Resource.newInstance(2 * 1024, 8);
    types[1] = new NodeInstanceType("a1.medium", r2, ModelType.ONDEMAND, 0.025);
    return types;
  }

  // Return int array. First element is the index of instance type. Second is
  // the minimumBuckets
  public static int[] getSuitableInstanceType(Resource res,
      NodeInstanceType[] allType, ResourceCalculator rc) {
    // assume it's better that a instance with resource approximate
    // to the requested container resource
    int[] ret = new int[2];
    int buckets = 0;
    int minimumBuckets = Integer.MAX_VALUE;
    int bestInstanceIndex = -1;
    for (int i = 0; i < allType.length; i++) {
      buckets = (int)rc.computeAvailableContainers(allType[i].getCapacity().getResource(), res);
      if (buckets > 0) {
        if (buckets < minimumBuckets) {
          minimumBuckets = buckets;
          bestInstanceIndex = i;
        }
      }
    }
    ret[0] = bestInstanceIndex;
    ret[1] = minimumBuckets;
    return ret;
  }

  public String toStr(int count) {
    return "name:" + this.modelName + "; count:" + count;
  }

}
