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


import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@XmlRootElement(name = "clusterScaling")
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterScalingInfo {

  public ClusterScalingInfo(){}

  public long getPendingMB() {
    return pendingMB;
  }

  public long getPendingVcore() {
    return pendingVcore;
  }

  public int getPendingContainersCount() {
    return pendingContainersCount;
  }

  public long getAvailableMB() {
    return availableMB;
  }

  public long getAvailableVcore() {
    return availableVcore;
  }

  public int getPendingAppCount() {
    return pendingAppCount;
  }

  protected int pendingAppCount;
  protected long pendingMB;
  protected long pendingVcore;
  protected int pendingContainersCount;
  protected long availableMB;
  protected long availableVcore;
  protected int recommendedExtraNMCount;

  public DecommissionCandidates getDecommissionCandidates() {
    return decommissionCandidates;
  }

  protected DecommissionCandidates decommissionCandidates =
      new DecommissionCandidates();

  public ClusterScalingInfo(final ResourceManager rm) {
    this(rm, rm.getResourceScheduler());
  }

  public ClusterScalingInfo(final ResourceManager rm, final ResourceScheduler rs) {
    if (rs == null) {
      throw new NotFoundException("Null ResourceScheduler instance");
    }

    QueueMetrics metrics = rs.getRootQueueMetrics();
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    this.pendingMB = metrics.getPendingMB();
    this.pendingVcore = metrics.getPendingVirtualCores();
    this.pendingAppCount = metrics.getAppsPending();
    this.pendingContainersCount = metrics.getPendingContainers();
    this.availableMB = metrics.getAvailableMB();
    this.availableVcore = metrics.getAvailableVirtualCores();
    Collection<RMNode> rmNodes =
        RMServerUtils.queryRMNodes(rm.getRMContext(), EnumSet.allOf(NodeState.class));

    Map<String, Integer> nodeToDecommissioningTimeoutSecs = new HashMap<>();

    for (RMNode rmNode : rmNodes) {
      // get remaining timeout
      Integer timeout = rmNode.getDecommissioningTimeout();
      if (timeout == null) {
        timeout = -1;
        nodeToDecommissioningTimeoutSecs.put(rmNode.getNodeID().toString(), timeout);
      }
    }
    // Zhankun TEST
    if (rmNodes.size() > 0) {
      Map<String, Integer> nodeToAppsRunningCount = new HashMap<>();
      Map<String, Integer> nodeToAMRunningCount = new HashMap<>();
      NodeId nId = ((List<RMNode>) rmNodes).get(0).getNodeID();
      rm.getResourceScheduler().getNumClusterNodes();
      FiCaSchedulerNode ficaNode = ((CapacityScheduler) rs)
          .getNode(nId);
      CandidateNodeSet<FiCaSchedulerNode> candidateNodeSet =
          ((CapacityScheduler) rs).getCandidateNodeSet(ficaNode);
      //ArrayList<SchedulerNode> nodelist = new ArrayList<>();
      for (SchedulerNode node : candidateNodeSet.getAllNodes().values()) {
        int amCount = 0;
        for (RMContainer rmContainer : node.getCopiedListOfRunningContainers() ) {
          // calculate AM count
          if (rmContainer.isAMContainer()) {
            amCount++;
          }
        }
        // calculate am count
        nodeToAMRunningCount.put(node.getNodeID().toString(), amCount);
        // calculate app running count
        nodeToAppsRunningCount.put(node.getNodeID().toString(),
            node.getRMNode().getRunningApps().size());
        //build list used for get iterator from multiNodeSortingManager
        //nodelist.add((node));
      }

      recommendedExtraNMCount = 0;
      for (RMNode rmNode : rmNodes){
        int amCount = nodeToAMRunningCount.getOrDefault(
            rmNode.getNodeID().toString(), 0);
        int runningAppCount = nodeToAppsRunningCount.getOrDefault(
            rmNode.getNodeID().toString(), 0);
        boolean recommendFlag = true;
        int keepNMCount = 0;
        if (amCount != 0 || runningAppCount != 0 ||
            rmNode.getState() == NodeState.DECOMMISSIONED ||
            rmNode.getState() == NodeState.DECOMMISSIONING ||
            rmNode.getState() == NodeState.SHUTDOWN) {
          recommendFlag = false;
          keepNMCount++;
        }
        int deTimeout = nodeToDecommissioningTimeoutSecs.getOrDefault(rmNode.getNodeID().toString(),
            -1);
        DecommissionCandidateNodeInfo dcni = new DecommissionCandidateNodeInfo(
            amCount,
            runningAppCount,
            deTimeout,
            rmNode.getState(),
            rmNode.getNodeID().toString(),
            recommendFlag
        );
        decommissionCandidates.add(dcni);
        // negative value to indicate scale down NM count
        recommendedExtraNMCount = keepNMCount - rmNodes.size();
      } // end for

      // if no scale down requirement, check scale up
      if (recommendedExtraNMCount == 0 &&
          pendingAppCount > 0 &&
          pendingContainersCount > 0) {
        // Assume uniform instance
        long instanceMB = ((List<RMNode>) rmNodes).get(0).getTotalCapability().getMemorySize();
        long instanceVcore = ((List<RMNode>) rmNodes).get(0).getTotalCapability().getVirtualCores();
        recommendedExtraNMCount = (int)Math.max(Math.ceil(pendingMB/instanceMB), Math.ceil(pendingVcore/instanceVcore));
        recommendedExtraNMCount = Math.max(1, recommendedExtraNMCount);
      }

//      String POLICY_CLASS_NAME =
//          "org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.ResourceUsageMultiNodeLookupPolicy";
//      Iterator<SchedulerNode> it = this.rm.getRMContext().getMultiNodeSortingManager()
//          .getMultiNodeSortIterator(nodelist,
//              "", POLICY_CLASS_NAME);
//      int decommissionCandidatesCount = 1;
//      int total = nodesInfo.getNodes().size();
//      int skip = total - decommissionCandidatesCount;
//      while (it.hasNext() && skip != 0) {
//        skip--;
//        it.next();
//      }
//      while (it.hasNext()) {
//        SchedulerNode e = it.next();
//        for (NodeInfo ni : nodesInfo.getNodes()) {
//          if (ni.getNodeId().equals(e.getNodeID().toString())) {
//            ni.setDecommissioningCandidates(true);
//          }
//        }
//      }
    }
  }

}
