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
import org.apache.hadoop.yarn.api.records.Resource;
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
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
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
  protected int pendingContainersCount;
  protected long availableMB;
  protected long availableVcore;
  protected CustomResourceInfo pendingResource;
  protected NodeInstanceType[] DefinedInstanceTypes = NodeInstanceType.getAllNodeInstanceType();;

  public NodeInstanceType[] getDefinedInstanceTypes() {
    return DefinedInstanceTypes;
  }

  public DecommissionCandidates getDecommissionCandidates() {
    return decommissionCandidates;
  }

  public NewNMCandidates getNewNMCandidates() {
    return newNMCandidates;
  }

  protected NewNMCandidates newNMCandidates = new NewNMCandidates();

  protected DecommissionCandidates decommissionCandidates =
      new DecommissionCandidates();

  public ClusterScalingInfo(final ResourceManager rm) {
    this(rm, rm.getResourceScheduler());
  }

  public ClusterScalingInfo(final ResourceManager rm, final ResourceScheduler rs) {
    if (rs == null) {
      throw new NotFoundException("Null ResourceScheduler instance");
    }
    newNMCandidates = new NewNMCandidates();
    QueueMetrics metrics = rs.getRootQueueMetrics();
    ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
    this.pendingAppCount = metrics.getAppsPending();
    this.pendingContainersCount = metrics.getPendingContainers();
    this.pendingResource = new CustomResourceInfo(metrics.getPendingResources());
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
      }
      // workaround the timeout accurate issue
      if (timeout > 0 && rmNode.getState() != NodeState.DECOMMISSIONING) {
        timeout = -1;
      }
      nodeToDecommissioningTimeoutSecs.put(rmNode.getNodeID().toString(), timeout);
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

      int keepNMCount = 0;
      for (RMNode rmNode : rmNodes){
        int amCount = nodeToAMRunningCount.getOrDefault(
            rmNode.getNodeID().toString(), 0);
        int runningAppCount = nodeToAppsRunningCount.getOrDefault(
            rmNode.getNodeID().toString(), 0);
        boolean recommendFlag = true;
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
      } // end for
      // if no scale down requirement, check scale up
      if (pendingAppCount > 0 ||
          pendingContainersCount > 0) {
        // given existing node types, found the maximum count of instance
        // that can serve the pending resource. Generally, the more instance,
        // the more opportunity to scale down
        StringBuilder tip = new StringBuilder();
        ResourceCalculator rc = new DominantResourceCalculator();
        Map<Resource, Integer> containerAskToCount = metrics.getContainerAskToCount();
        int[] suitableInstanceRet = null;
        NodeInstanceType[] allTypes = getDefinedInstanceTypes();
        for (Map.Entry<Resource, Integer> entry : containerAskToCount.entrySet()) {
          suitableInstanceRet = NodeInstanceType.getSuitableInstanceType(
              entry.getKey(), allTypes, rc);
          int ti = suitableInstanceRet[0];
          if (ti == -1) {
            tip.append(String.format(
                "No capable instance type for container resource: %s, count: %d",
                entry.getKey(), entry.getValue()));
          } else {
            NodeInstanceType t = allTypes[ti];
            int containerBuckets = suitableInstanceRet[1];
            newNMCandidates.add(t, (int)Math.ceil((double)entry.getValue()/(double)containerBuckets));
          }
        }
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
