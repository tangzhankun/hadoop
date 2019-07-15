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
import org.apache.hadoop.yarn.api.records.ResourceInformation;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

@XmlRootElement(name = "CustomResourceInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class CustomResourceInfo {
  protected long mem;

  public long getMem() {
    return mem;
  }

  public long getVcore() {
    return vcore;
  }

  protected long vcore;

  public ArrayList<CustomResourceType> getCustomResourceTypeList() {
    return customResourceTypeList;
  }

  protected ArrayList<CustomResourceType> customResourceTypeList = new ArrayList();

  public CustomResourceInfo() {}

  public CustomResourceInfo(Resource r) {
    this.mem = r.getMemorySize();
    this.vcore = r.getVirtualCores();
    if (this.customResourceTypeList == null) {
      this.customResourceTypeList = new ArrayList<>();
    }
    for (ResourceInformation ri : r.getResources()) {
      if (ri.getName().equals(ResourceInformation.MEMORY_MB.getName()) ||
          ri.getName().equals(ResourceInformation.VCORES.getName())) {
        continue;
      }
      this.customResourceTypeList.add(new CustomResourceType(ri.getName(), ri.getValue()));
    }
  }
}
