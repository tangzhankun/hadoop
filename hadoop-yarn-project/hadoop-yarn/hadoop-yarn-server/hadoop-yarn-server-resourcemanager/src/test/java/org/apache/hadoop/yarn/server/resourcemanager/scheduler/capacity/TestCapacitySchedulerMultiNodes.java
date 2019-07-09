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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSorter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for Multi Node scheduling related tests.
 */
public class TestCapacitySchedulerMultiNodes extends CapacitySchedulerTestBase {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCapacitySchedulerMultiNodes.class);
  private CapacitySchedulerConfiguration conf;
  private static final String POLICY_CLASS_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy";

  @Before
  public void setUp() {
    CapacitySchedulerConfiguration config =
        new CapacitySchedulerConfiguration();
    config.set(CapacitySchedulerConfiguration.RESOURCE_CALCULATOR_CLASS,
        DominantResourceCalculator.class.getName());
    conf = new CapacitySchedulerConfiguration(config);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICIES,
        "resource-based");
    conf.set(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME,
        "resource-based");
    String policyName =
        CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
            + ".resource-based" + ".class";
    conf.set(policyName, POLICY_CLASS_NAME);
    conf.setBoolean(CapacitySchedulerConfiguration.MULTI_NODE_PLACEMENT_ENABLED,
        true);
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 512);
    conf.setInt("yarn.scheduler.minimum-allocation-vcores", 1);
  }

  @Test
  public void testMultiNodeSorterForScheduling() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    rm.registerNode("127.0.0.1:1234", 10 * GB);
    rm.registerNode("127.0.0.1:1235", 10 * GB);
    rm.registerNode("127.0.0.1:1236", 10 * GB);
    rm.registerNode("127.0.0.1:1237", 10 * GB);
    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 4, 5);
    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();
    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    Assert.assertEquals(4, nodes.size());
    rm.stop();
  }

  // Zhankun
  @Test
  public void testMultiNodeSorterForSchedulingWithOrdering() throws Exception {
    MockRM rm = new MockRM(conf);
    rm.start();
    MockNM[] nms = new MockNM[4];
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5 * GB, 100);
    MockNM nm2 = rm.registerNode("127.0.0.2:1235", 100 * GB, 100);
    MockNM nm3 = rm.registerNode("127.0.0.3:1236", 100 * GB, 100);
    MockNM nm4 = rm.registerNode("127.0.0.4:1237", 100 * GB, 100);

    nms[0] = nm4;
    nms[1] = nm3;
    nms[2] = nm2;
    nms[3] = nm1;

    ResourceScheduler scheduler = rm.getRMContext().getScheduler();
    waitforNMRegistered(scheduler, 4, 5);

    MultiNodeSortingManager<SchedulerNode> mns = rm.getRMContext()
        .getMultiNodeSortingManager();
    MultiNodeSorter<SchedulerNode> sorter = mns
        .getMultiNodePolicy(POLICY_CLASS_NAME);
    sorter.reSortClusterNodes();

    Set<SchedulerNode> nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    Assert.assertEquals(4, nodes.size());

    RMApp app1 = rm.submitApp(5 * GB, "app-1", "user1", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm, nms);
    SchedulerNodeReport reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());

    // check node report
    Assert.assertEquals(5 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals((5 - 5) * GB,
        reportNm1.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    RMApp app2 = rm.submitApp(3 * GB, "app-2", "user2", null, "default");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm, nms);
    Resource cResource = Resources.createResource(1 * GB, 1);
    am2.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);
    heartbeat(rm, nm2);
    // check node report
    SchedulerNodeReport reportNm2 =
        rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(4 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 4) * GB,
        reportNm2.getAvailableResource().getMemorySize());

    // check node report
    reportNm1 =
        rm.getResourceScheduler().getNodeReport(nm1.getNodeId());
    Assert.assertEquals(5 * GB, reportNm1.getUsedResource().getMemorySize());
    Assert.assertEquals(0 * GB,
        reportNm1.getAvailableResource().getMemorySize());

    // am request containers
    cResource = Resources.createResource(8 * GB, 1);
    am1.allocate("*", cResource, 1,
        new ArrayList<ContainerId>(), null);

    heartbeat(rm, nm1);
    heartbeat(rm, nm2);
    heartbeat(rm, nm3);
    heartbeat(rm, nm4);

    // check node report
    reportNm2 =
        rm.getResourceScheduler().getNodeReport(nm2.getNodeId());
    Assert.assertEquals(12 * GB, reportNm2.getUsedResource().getMemorySize());
    Assert.assertEquals((100 - 12) * GB,
        reportNm2.getAvailableResource().getMemorySize());

    SchedulerNodeReport reportNm3 =
        rm.getResourceScheduler().getNodeReport(nm3.getNodeId());
    // check node report
    Assert.assertEquals(0 * GB, reportNm3.getUsedResource().getMemorySize());
    Assert.assertEquals(100 * GB,
        reportNm3.getAvailableResource().getMemorySize());

    SchedulerNodeReport reportNm4 =
        rm.getResourceScheduler().getNodeReport(nm4.getNodeId());
    // check node report
    Assert.assertEquals(0 * GB, reportNm4.getUsedResource().getMemorySize());
    Assert.assertEquals(100 * GB,
        reportNm4.getAvailableResource().getMemorySize());

    // Ideally thread will invoke this, but thread operates every 1sec.
    // Hence forcefully recompute nodes.
    sorter.reSortClusterNodes();

    nodes = sorter.getMultiNodeLookupPolicy()
        .getNodesPerPartition("");
    List<NodeId> currentNodes = new ArrayList<>();
    currentNodes.add(nm2.getNodeId());
    currentNodes.add(nm1.getNodeId());
    currentNodes.add(nm3.getNodeId());
    currentNodes.add(nm4.getNodeId());
    Iterator<SchedulerNode> it = nodes.iterator();
    SchedulerNode current;
    int i = 0;
    while (it.hasNext()) {
      current = it.next();
      Assert.assertEquals(current.getNodeID(), currentNodes.get(i++));
    }
    rm.stop();
  }


  private void heartbeat(MockRM rm, MockNM nm) {
    RMNode node = rm.getRMContext().getRMNodes().get(nm.getNodeId());
    // Send a heartbeat to kick the tires on the Scheduler
    NodeUpdateSchedulerEvent nodeUpdate = new NodeUpdateSchedulerEvent(node);
    rm.getResourceScheduler().handle(nodeUpdate);
  }

  @Test (timeout=30000)
  public void testExcessReservationWillBeUnreserved() throws Exception {
    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);
    newConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    newConf.setInt(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
        + ".resource-based.sorting-interval.ms", 0);
    newConf.setMaximumApplicationMasterResourcePerQueuePercent("root.default",
        1.0f);
    MockRM rm1 = new MockRM(newConf);

    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue, AM container should be launched in nm1
    RMApp app1 = rm1.submitApp(5 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm2
    RMApp app2 = rm1.submitApp(5 * GB, "app", "user", null, "default");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    LeafQueue leafQueue = (LeafQueue) cs.getQueue("default");
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    /*
     * Verify that reserved container will be unreserved
     * after its ask has been cancelled when used capacity of root queue is 1.
     */
    // Ask a container with 6GB memory size for app1,
    // nm1 will reserve a container for app1
    am1.allocate("*", 6 * GB, 1, new ArrayList<>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // Check containers of app1 and app2.
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp1.getReservedContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    // Cancel ask of the reserved container.
    am1.allocate("*", 6 * GB, 0, new ArrayList<>());
    // Ask another container with 2GB memory size for app2.
    am2.allocate("*", 2 * GB, 1, new ArrayList<>());

    // Trigger scheduling to release reserved container
    // whose ask has been cancelled.
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(0, schedulerApp1.getReservedContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());

    // Trigger scheduling to allocate a container on nm1 for app2.
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(0, schedulerApp1.getReservedContainers().size());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(7 * GB,
        cs.getNode(nm1.getNodeId()).getAllocatedResource().getMemorySize());
    Assert.assertEquals(12 * GB,
        cs.getRootQueue().getQueueResourceUsage().getUsed().getMemorySize());
    Assert.assertEquals(0,
        cs.getRootQueue().getQueueResourceUsage().getReserved()
            .getMemorySize());
    Assert.assertEquals(0,
        leafQueue.getQueueResourceUsage().getReserved().getMemorySize());

    rm1.close();
  }

  @Test(timeout=30000)
  public void testAllocateForReservedContainer() throws Exception {
    CapacitySchedulerConfiguration newConf =
        new CapacitySchedulerConfiguration(conf);
    newConf.set(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER,
        YarnConfiguration.SCHEDULER_RM_PLACEMENT_CONSTRAINTS_HANDLER);
    newConf.setInt(CapacitySchedulerConfiguration.MULTI_NODE_SORTING_POLICY_NAME
        + ".resource-based.sorting-interval.ms", 0);
    newConf.setMaximumApplicationMasterResourcePerQueuePercent("root.default",
        1.0f);
    MockRM rm1 = new MockRM(newConf);

    rm1.start();
    MockNM nm1 = rm1.registerNode("h1:1234", 8 * GB);
    MockNM nm2 = rm1.registerNode("h2:1234", 8 * GB);

    // launch an app to queue, AM container should be launched in nm1
    RMApp app1 = rm1.submitApp(5 * GB, "app", "user", null, "default");
    MockAM am1 = MockRM.launchAndRegisterAM(app1, rm1, nm1);

    // launch another app to queue, AM container should be launched in nm2
    RMApp app2 = rm1.submitApp(5 * GB, "app", "user", null, "default");
    MockAM am2 = MockRM.launchAndRegisterAM(app2, rm1, nm2);

    CapacityScheduler cs = (CapacityScheduler) rm1.getResourceScheduler();
    RMNode rmNode1 = rm1.getRMContext().getRMNodes().get(nm1.getNodeId());
    FiCaSchedulerApp schedulerApp1 =
        cs.getApplicationAttempt(am1.getApplicationAttemptId());
    FiCaSchedulerApp schedulerApp2 =
        cs.getApplicationAttempt(am2.getApplicationAttemptId());

    /*
     * Verify that reserved container will be allocated
     * after node has sufficient resource.
     */
    // Ask a container with 6GB memory size for app2,
    // nm1 will reserve a container for app2
    am2.allocate("*", 6 * GB, 1, new ArrayList<>());
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));

    // Check containers of app1 and app2.
    Assert.assertNotNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(1, schedulerApp1.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getLiveContainers().size());
    Assert.assertEquals(1, schedulerApp2.getReservedContainers().size());

    // Kill app1 to release resource on nm1.
    rm1.killApp(app1.getApplicationId());

    // Trigger scheduling to allocate for reserved container on nm1.
    cs.handle(new NodeUpdateSchedulerEvent(rmNode1));
    Assert.assertNull(cs.getNode(nm1.getNodeId()).getReservedContainer());
    Assert.assertEquals(2, schedulerApp2.getLiveContainers().size());

    rm1.close();
  }
}
