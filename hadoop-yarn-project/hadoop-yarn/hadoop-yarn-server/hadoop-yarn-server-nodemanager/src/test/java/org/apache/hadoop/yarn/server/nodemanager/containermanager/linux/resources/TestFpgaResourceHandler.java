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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.Fpga.AbstractFpgaPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.Fpga.FpgaResourceHandlerImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestFpgaResourceHandler {
  private FpgaResourceHandlerImpl fpgaResourceHandler;
  private Configuration configuration;

  /**
   * it's better to define allowed devices in the node-resource.xml:
   * <property>
   *   <name>yarn.nodemanager.resource-types.MCP</name>
   *   <value>2</value>
   * </property>
   * <property>
   *   <name>yarn.nodemanager.resource-types.MCP.allowed</name>
   *   <value>244:0,245:1</value>
   * </property>
   * <property>
   *   <name>yarn.nodemanager.resource-types.DCP</name>
   *   <value>2</value>
   * </property>
   * <property>
   *   <name>yarn.nodemanager.resource-types.DCP.allowed</name>
   *   <value>100:0,100:1</value>
   * </property>
   */
  @Before
  public void setup() {
    configuration = new YarnConfiguration();
    fpgaResourceHandler = new FpgaResourceHandlerImpl(mock(CGroupsHandler.class), configuration);
    configuration.set(YarnConfiguration.NM_RESOURCES_PREFIX + "MCP.allowed", "244:0,245:1");
  }

  @Test
  public void testBootstrap() throws ResourceHandlerException {
    fpgaResourceHandler.bootstrap(configuration);
    Assert.assertEquals(1,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpga().keySet().size());
    Assert.assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());

  }

  @Test
  public void testPreStartWithOnePlugin() throws ResourceHandlerException {
    fpgaResourceHandler.bootstrap(configuration);
    //the id-0 container request 1 FPGA of MCP type and GEMM IP
    fpgaResourceHandler.preStart(mockContainer(0,"MCP",1,"GEMM"));
    Assert.assertEquals(1, fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    //the id-1 container request 2 FPGA of MCP and GEMM IP. this should failed
    try{
      fpgaResourceHandler.preStart(mockContainer(1,"MCP",2,"GEMM"));
    } catch (ResourceHandlerException e) {
      Assert.assertTrue(true);
    }
    //release the id-0 container
    fpgaResourceHandler.postComplete(getContainerId(0));
    Assert.assertEquals(0, fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    Assert.assertEquals(2, fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    //re-allocate for the id-1 container
    fpgaResourceHandler.preStart(mockContainer(1,"MCP",2,"GEMM"));
    Assert.assertEquals(2, fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    Assert.assertEquals(0, fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    //release container id-1
    fpgaResourceHandler.postComplete(getContainerId(1));
    //id-2 and id-3
    fpgaResourceHandler.preStart(mockContainer(2,"MCP",1,"GEMM"));
    fpgaResourceHandler.preStart(mockContainer(3,"MCP",1,"GEMM"));
    Assert.assertEquals(2, fpgaResourceHandler.getFpgaAllocator().getUsedFpgaCount());
    Assert.assertEquals(0, fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    fpgaResourceHandler.postComplete(getContainerId(2));
    fpgaResourceHandler.postComplete(getContainerId(3));

  }

  @Test
  public void testPreStartWithMultiplePlugins() throws ResourceHandlerException {
    fpgaResourceHandler.addFpgaPlugin(mockPlugin("DCP"));
    configuration.set(YarnConfiguration.NM_RESOURCES_PREFIX + "DCP.allowed", "100:0,100:1");
    fpgaResourceHandler.bootstrap(configuration);
    Assert.assertEquals(2,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpga().keySet().size());
    Assert.assertEquals(4,
        fpgaResourceHandler.getFpgaAllocator().getAvailableFpgaCount());
    fpgaResourceHandler.preStart(mockContainer(0,"MCP",2,"GEMM"));
    fpgaResourceHandler.preStart(mockContainer(1,"DCP",1,"LZO"));
    Assert.assertEquals(0, fpgaResourceHandler.getFpgaAllocator().getAvailableFpga().get("MCP").size());
    Assert.assertEquals(2, fpgaResourceHandler.getFpgaAllocator().getUsedFpga().get(getContainerId(0).toString()).size());
    Assert.assertEquals(1, fpgaResourceHandler.getFpgaAllocator().getAvailableFpga().get("DCP").size());
    Assert.assertEquals(1, fpgaResourceHandler.getFpgaAllocator().getUsedFpga().get(getContainerId(1).toString()).size());
  }

  private static AbstractFpgaPlugin mockPlugin(String type) {
    AbstractFpgaPlugin plugin = mock(AbstractFpgaPlugin.class);
    when(plugin.initPlugin()).thenReturn(true);
    when(plugin.getFpgaType()).thenReturn(type);
    when(plugin.getExistingIPID(Mockito.anyInt(), Mockito.anyInt())).thenReturn("LZO");
    when(plugin.cleanupFpgas(Mockito.anyObject())).thenReturn(true);
    when(plugin.downloadIP(Mockito.anyString(), Mockito.anyString())).thenReturn("/tmp");
    when(plugin.configureIP(Mockito.anyString(), Mockito.anyObject())).thenReturn(true);
    return plugin;
  }

  private static Container mockContainer(int id, String type, int numFpga, String IPID) {
    Container c = mock(Container.class);
    ResourceInformation resourceInformation = ResourceInformation.newInstance(type,numFpga);
    Map<String, ResourceInformation> map = new HashMap<>();
    map.put(type,resourceInformation);
    Resource r = mock(Resource.class);
    when(c.getResource()).thenReturn(r);
    when(r.getResources()).thenReturn(map);
    when(c.getContainerId()).thenReturn(getContainerId(id));
    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    Map<String, String> envs = new HashMap<>();
    envs.put("REQUESTED_FPGA_IP_ID", IPID);
    when(c.getLaunchContext()).thenReturn(clc);
    when(clc.getEnvironment()).thenReturn(envs);
    when(c.getWorkDir()).thenReturn("/tmp");
    return c;
  }

  private static ContainerId getContainerId(int id) {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.newInstance(1234L, 1), 1), id);
  }
}
