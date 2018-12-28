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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.TypedResourceUtilization;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceUtilizationPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TypedResourceUtilizationPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceInformationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.TypedResourceUtilizationProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceUtilizationProto;
import org.apache.hadoop.yarn.server.nodemanager.containermanager
    .BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager
    .monitor.MockResourceCalculatorPlugin;

import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;

public class TestNodeResourceMonitor extends BaseContainerManagerTest {
  public TestNodeResourceMonitor() throws UnsupportedFileSystemException {
    super();
  }

  @Before
  public void setup() throws IOException {
    // Enable node resource monitor with a mocked resource calculator.
    conf.set(
        YarnConfiguration.NM_MON_RESOURCE_CALCULATOR,
        MockResourceCalculatorPlugin.class.getCanonicalName());
    Configuration conf = new YarnConfiguration();
    ResourceUtils.resetResourceTypes();

    String resourceTypesFile = "resource-types-pluggable-devices.xml";
    try {
      String toBeDelete = TestResourceUtils.setupResourceTypes(conf,
          resourceTypesFile);
      new File(toBeDelete).deleteOnExit();
    } catch (Exception e) {
      e.printStackTrace();
    }
    super.setup();
  }

  @Test
  public void testMetricsUpdate() throws Exception {
    // This test doesn't verify the correction of those metrics
    // updated by the monitor, it only verifies that the monitor
    // do publish these info to node manager metrics system in
    // each monitor interval.
    Context spyContext = spy(context);
    NodeResourceMonitor nrm = new NodeResourceMonitorImpl(spyContext);
    nrm.init(conf);
    nrm.start();
    Mockito.verify(spyContext, timeout(500).atLeastOnce())
        .getNodeManagerMetrics();
  }

  @Test
  public void testResourceUtilizationGetterAndSetter() {
    // Case 1. Set typed resource in utilization
    ResourceUtilization nodeUtilization = ResourceUtilization.newInstance(
        1024, 1024, 2);
    String resourceType = "sample.com/sample";
    nodeUtilization.setResourceValue(resourceType, 20);
    nodeUtilization.setResourceValue(resourceType, 10);
    nodeUtilization.setUsedResourceValue(resourceType, 5);
    Assert.assertEquals(
        nodeUtilization.getResourceValue(resourceType)
        , 10);
    Assert.assertEquals(
        nodeUtilization.getUsedResourceValue(resourceType)
        , 5);
    ResourceUtilizationProto proto =
        ((ResourceUtilizationPBImpl)nodeUtilization).getProto();
    List<TypedResourceUtilizationProto> tProtos =
        proto.getTypedResourcesList();
    for (TypedResourceUtilizationProto tProto : tProtos) {
      if (tProto.getCapability().getKey().equals(resourceType)) {
        Assert.assertEquals(10, tProto.getCapability().getValue());
        Assert.assertEquals(5, tProto.getUsed());
      }
    }
    // Case 2. No typed resource
    ResourceUtilization nodeUtilization2 = ResourceUtilization.newInstance(
        1024, 1024, 2);
    ResourceUtilizationProto proto2 =
        ((ResourceUtilizationPBImpl)nodeUtilization2).getProto();
    List<TypedResourceUtilizationProto> tProtos2 =
        proto2.getTypedResourcesList();
    Assert.assertEquals(0, tProtos2.size());
  }

  @Test
  public void testResourceUtilizationProto() {
    // Case 1. Use Proto to init TypedResourceUtilization
    String resourceType = "sample.com/sample";
    ResourceInformationProto.Builder riPB =
        ResourceInformationProto.newBuilder();
    riPB.setKey(resourceType);
    riPB.setValue(10);
    TypedResourceUtilizationProto.Builder truPB =
        TypedResourceUtilizationProto.newBuilder();
    truPB.setUsed(5);
    truPB.setCapability(riPB.build());
    ResourceUtilizationProto.Builder ruPB =
        ResourceUtilizationProto.newBuilder();
    ruPB.addTypedResources(0, truPB.build());
    ResourceUtilization ru = new ResourceUtilizationPBImpl(ruPB.build());

    Assert.assertEquals(10, ru.getResourceValue(resourceType));
    Assert.assertEquals(5, ru.getUsedResourceValue(resourceType));

    ru.setResourceValue(resourceType, 20);
    ru.setUsedResourceValue(resourceType, 10);
    Assert.assertEquals(
        ru.getResourceValue(resourceType)
        , 20);
    Assert.assertEquals(
        ru.getUsedResourceValue(resourceType)
        , 10);
  }

}
