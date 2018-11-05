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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;


import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.*;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.examples.FakeDevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.TestResourceUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class TestDevicePluginAdapter {

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestDevicePluginAdapter.class);

  private YarnConfiguration conf;
  private String tempResourceTypesFile;
  private CGroupsHandler mockCGroupsHandler;
  private PrivilegedOperationExecutor mockPrivilegedExecutor;
  private NodeManager nm;

  @Before
  public void setup() throws Exception {
    this.conf = new YarnConfiguration();
    // setup resource-types.xml
    ResourceUtils.resetResourceTypes();
    String resourceTypesFile = "resource-types-pluggable-devices.xml";
    this.tempResourceTypesFile = TestResourceUtils.setupResourceTypes(this.conf, resourceTypesFile);
    mockCGroupsHandler = mock(CGroupsHandler.class);
    mockPrivilegedExecutor = mock(PrivilegedOperationExecutor.class);
  }

  @After
  public void tearDown() throws IOException {
    // cleanup resource-types.xml
    File dest = new File(this.tempResourceTypesFile);
    if (dest.exists()) {
      dest.delete();
    }
    if (nm != null) {
      try {
        ServiceOperations.stop(nm);
      } catch (Throwable t) {
        // ignore
      }
    }
  }


  /**
   * Use the MyPlugin which doesn't implement scheduler interfaces
   * Plugin's initialization is tested in
   * {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.TestResourcePluginManager}
   *
   * */
  @Test
  public void testBasicWorkflow()
      throws YarnException, IOException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceSchedulerManager dsm = new DeviceSchedulerManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceSchedulerManager()).thenReturn(dsm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.resourceName;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dsm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    int size = dsm.getAvailableDevices(resourceName);
    Assert.assertEquals(3, size);

    // A container c1 requests 1 device
    Container c1 = mockContainerWithDeviceRequest(0,
        resourceName,
        1,false);
    // preStart
    adapter.getDeviceResourceHandler().preStart(c1);
    // check book keeping
    Assert.assertEquals(2,
        dsm.getAvailableDevices(resourceName));
    Assert.assertEquals(1,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dsm.getAllAllowedDevices().get(resourceName).size());
    // postComplete
    adapter.getDeviceResourceHandler().postComplete(getContainerId(0));
    Assert.assertEquals(3,
        dsm.getAvailableDevices(resourceName));
    Assert.assertEquals(0,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dsm.getAllAllowedDevices().get(resourceName).size());

    // A container c2 requests 3 device
    Container c2 = mockContainerWithDeviceRequest(1,
        resourceName,
        3,false);
    // preStart
    adapter.getDeviceResourceHandler().preStart(c2);
    // check book keeping
    Assert.assertEquals(0,
        dsm.getAvailableDevices(resourceName));
    Assert.assertEquals(3,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dsm.getAllAllowedDevices().get(resourceName).size());
    // postComplete
    adapter.getDeviceResourceHandler().postComplete(getContainerId(1));
    Assert.assertEquals(3,
        dsm.getAvailableDevices(resourceName));
    Assert.assertEquals(0,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dsm.getAllAllowedDevices().get(resourceName).size());
  }

  /**
   * Use {@link FakeDevicePlugin} which implements
   * {@link org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePluginScheduler}
   * interface.
   *
   * */
  @Test
  public void testBasicWorkflowWithPluginAndPluginScheduler()
      throws YarnException, IOException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService storeService = mock(NMStateStoreService.class);
    when(context.getNMStateStore()).thenReturn(storeService);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));
    // Init scheduler manager
    DeviceSchedulerManager dsm = new DeviceSchedulerManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceSchedulerManager()).thenReturn(dsm);

    // Init an plugin
    FakeDevicePlugin plugin = new FakeDevicePlugin();
    FakeDevicePlugin spyPlugin = spy(plugin);
    String resourceName = FakeDevicePlugin.resourceName;
    // Add customized device plugin scheduler
    dsm.setPreferCustomizedScheduler(true);
    dsm.addDevicePluginScheduler(resourceName,spyPlugin);
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dsm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    int size = dsm.getAvailableDevices(resourceName);
    Assert.assertEquals(1, size);

    // A container requests 1 device
    Container c1 = mockContainerWithDeviceRequest(0,
        resourceName,
        1,false);
    // preStart
    adapter.getDeviceResourceHandler().preStart(c1);
    // check if the plugin's own scheduler works
    verify(spyPlugin, times(1))
        .allocateDevices(isA(Set.class),isA(Integer.class));
    // check book keeping
    Assert.assertEquals(0,
        dsm.getAvailableDevices(resourceName));
    Assert.assertEquals(1,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(1,
        dsm.getAllAllowedDevices().get(resourceName).size());
    // postComplete
    adapter.getDeviceResourceHandler().postComplete(getContainerId(0));
    Assert.assertEquals(1,
        dsm.getAvailableDevices(resourceName));
    Assert.assertEquals(0,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(1,
        dsm.getAllAllowedDevices().get(resourceName).size());
  }

  @Test
  public void testStoreDeviceSchedulerManagerState()
      throws IOException, YarnException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService realStoreService = new NMMemoryStateStoreService();
    NMStateStoreService storeService = spy(realStoreService);
    when(context.getNMStateStore()).thenReturn(storeService);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceSchedulerManager dsm = new DeviceSchedulerManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceSchedulerManager()).thenReturn(dsm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.resourceName;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dsm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);

    // A container c0 requests 1 device
    Container c0 = mockContainerWithDeviceRequest(0,
        resourceName,
        1,false);
    // preStart
    adapter.getDeviceResourceHandler().preStart(c0);
    // ensure container1's resource is persistent
    verify(storeService).storeAssignedResources(c0, resourceName,
        Arrays.asList(Device.Builder.newInstance()
            .setID(0)
            .setDevPath("/dev/hdwA0")
            .setMajorNumber(256)
            .setMinorNumber(0)
            .setBusID("0000:80:00.0")
            .setHealthy(true)
            .build()));
  }

  @Test
  public void testRecoverDeviceSchedulerManagerState() throws IOException, YarnException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService realStoreService = new NMMemoryStateStoreService();
    NMStateStoreService storeService = spy(realStoreService);
    when(context.getNMStateStore()).thenReturn(storeService);
    doNothing().when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceSchedulerManager dsm = new DeviceSchedulerManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceSchedulerManager()).thenReturn(dsm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.resourceName;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dsm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);
    Assert.assertEquals(3,
        dsm.getAllAllowedDevices().get(resourceName).size());
    // mock NMStateStore
    Device storedDevice = Device.Builder.newInstance()
        .setID(0)
        .setDevPath("/dev/hdwA0")
        .setMajorNumber(256)
        .setMinorNumber(0)
        .setBusID("0000:80:00.0")
        .setHealthy(true)
        .build();
    ConcurrentHashMap<ContainerId, Container> runningContainersMap
        = new ConcurrentHashMap<>();
    Container nmContainer = mock(Container.class);
    ResourceMappings rmap = new ResourceMappings();
    ResourceMappings.AssignedResources ar =
        new ResourceMappings.AssignedResources();
    ar.updateAssignedResources(
        Arrays.asList(storedDevice));
    rmap.addAssignedResources(resourceName, ar);
    when(nmContainer.getResourceMappings()).thenReturn(rmap);
    when(context.getContainers()).thenReturn(runningContainersMap);

    // Test case 1. c0 get recovered. scheduler state restored
    runningContainersMap.put(getContainerId(0), nmContainer);
    adapter.getDeviceResourceHandler().reacquireContainer(
        getContainerId(0));
    Assert.assertEquals(3,
        dsm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(1,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(2,
        dsm.getAvailableDevices(resourceName));
    Map<Device, ContainerId> used = dsm.getAllUsedDevices().get(resourceName);
    Assert.assertTrue(used.keySet().contains(storedDevice));

    // Test case 2. c1 wants get recovered. But stored device is already allocated to c2
    nmContainer = mock(Container.class);
    rmap = new ResourceMappings();
    ar = new ResourceMappings.AssignedResources();
    ar.updateAssignedResources(
        Arrays.asList(storedDevice));
    rmap.addAssignedResources(resourceName, ar);
    // already assigned to c1
    runningContainersMap.put(getContainerId(2), nmContainer);
    boolean caughtException = false;
    try {
      adapter.getDeviceResourceHandler().reacquireContainer(getContainerId(1));
    } catch (ResourceHandlerException e) {
      caughtException = true;
    }
    Assert.assertTrue(
        "Should fail since requested device is assigned already",
        caughtException);
    // don't affect c0 allocation state
    Assert.assertEquals(3,
        dsm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(1,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(2,
        dsm.getAvailableDevices(resourceName));
    used = dsm.getAllUsedDevices().get(resourceName);
    Assert.assertTrue(used.keySet().contains(storedDevice));
  }

  @Test
  public void testAssignedDeviceCleanupWhenStoreOpFails() throws IOException, YarnException {
    NodeManager.NMContext context = mock(NodeManager.NMContext.class);
    NMStateStoreService realStoreService = new NMMemoryStateStoreService();
    NMStateStoreService storeService = spy(realStoreService);
    when(context.getNMStateStore()).thenReturn(storeService);
    doThrow(new IOException("Exception ...")).when(storeService).storeAssignedResources(isA(Container.class),
        isA(String.class),
        isA(ArrayList.class));

    // Init scheduler manager
    DeviceSchedulerManager dsm = new DeviceSchedulerManager(context);

    ResourcePluginManager rpm = mock(ResourcePluginManager.class);
    when(rpm.getDeviceSchedulerManager()).thenReturn(dsm);

    // Init an plugin
    MyPlugin plugin = new MyPlugin();
    MyPlugin spyPlugin = spy(plugin);
    String resourceName = MyPlugin.resourceName;
    // Init an adapter for the plugin
    DevicePluginAdapter adapter = new DevicePluginAdapter(
        resourceName,
        spyPlugin, dsm);
    // Bootstrap, adding device
    adapter.initialize(context);
    adapter.createResourceHandler(context,
        mockCGroupsHandler, mockPrivilegedExecutor);
    adapter.getDeviceResourceHandler().bootstrap(conf);

    // A container c0 requests 1 device
    Container c0 = mockContainerWithDeviceRequest(0,
        resourceName,
        1,false);
    // preStart
    boolean exception = false;
    try {
      adapter.getDeviceResourceHandler().preStart(c0);
    } catch (ResourceHandlerException e) {
      exception = true;
    }
    Assert.assertTrue("Should throw exception in preStart", exception);
    // no device assigned
    Assert.assertEquals(3,
        dsm.getAllAllowedDevices().get(resourceName).size());
    Assert.assertEquals(0,
        dsm.getAllUsedDevices().get(resourceName).size());
    Assert.assertEquals(3,
        dsm.getAvailableDevices(resourceName));

  }

  private static Container mockContainerWithDeviceRequest(int id,
      String resourceName,
      int numDeviceRequest,
      boolean dockerContainerEnabled) {
    Container c = mock(Container.class);
    when(c.getContainerId()).thenReturn(getContainerId(id));

    Resource res = Resource.newInstance(1024, 1);
    ResourceMappings resMapping = new ResourceMappings();

    res.setResourceValue(resourceName, numDeviceRequest);
    when(c.getResource()).thenReturn(res);
    when(c.getResourceMappings()).thenReturn(resMapping);

    ContainerLaunchContext clc = mock(ContainerLaunchContext.class);
    Map<String, String> env = new HashMap<>();
    if (dockerContainerEnabled) {
      env.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE,
          ContainerRuntimeConstants.CONTAINER_RUNTIME_DOCKER);
    }
    when(clc.getEnvironment()).thenReturn(env);
    when(c.getLaunchContext()).thenReturn(clc);
    return c;
  }

  private static ContainerId getContainerId(int id) {
    return ContainerId.newContainerId(ApplicationAttemptId
        .newInstance(ApplicationId.newInstance(1234L, 1), 1), id);
  }


  private class MyPlugin implements DevicePlugin {
    private final static String resourceName = "cmpA.com/hdwA";
    @Override
    public DeviceRegisterRequest getRegisterRequestInfo() {
      return DeviceRegisterRequest.Builder.newInstance()
          .setResourceName(resourceName)
          .setPluginVersion("v1.0").build();
    }

    @Override
    public Set<Device> getDevices() {
      TreeSet<Device> r = new TreeSet<>();
      r.add(Device.Builder.newInstance()
          .setID(0)
          .setDevPath("/dev/hdwA0")
          .setMajorNumber(256)
          .setMinorNumber(0)
          .setBusID("0000:80:00.0")
          .setHealthy(true)
          .build());
      r.add(Device.Builder.newInstance()
          .setID(1)
          .setDevPath("/dev/hdwA1")
          .setMajorNumber(256)
          .setMinorNumber(0)
          .setBusID("0000:80:01.0")
          .setHealthy(true)
          .build());
      r.add(Device.Builder.newInstance()
          .setID(2)
          .setDevPath("/dev/hdwA2")
          .setMajorNumber(256)
          .setMinorNumber(0)
          .setBusID("0000:80:02.0")
          .setHealthy(true)
          .build());
      return r;
    }

    @Override
    public DeviceRuntimeSpec onDevicesAllocated(Set<Device> allocatedDevices, String runtime) {
      return null;
    }

    @Override
    public void onDevicesReleased(Set<Device> releasedDevices) {

    }
  } // MyPlugin

}
