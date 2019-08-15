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
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@XmlRootElement(name = "clusterScaling")
@XmlAccessorType(XmlAccessType.NONE)
public class ClusterScalingInfo {

  public ClusterScalingInfo(){}

  public int getPendingContainersCount() {
    return pendingContainersCount;
  }

  public int getPendingAppCount() {
    return pendingAppCount;
  }

  protected int pendingAppCount;
  protected int pendingContainersCount;

  public CustomResourceInfo getAvailableResource() {
    return availableResource;
  }

  protected CustomResourceInfo availableResource;
  protected CustomResourceInfo pendingResource;

  public DecommissionCandidates getDecommissionCandidates() {
    return decommissionCandidates;
  }

  public NewNMCandidates getNewNMCandidates() {
    return newNMCandidates;
  }

  @XmlElement
  protected NewNMCandidates newNMCandidates = new NewNMCandidates();

  @XmlElement
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
    this.availableResource = new CustomResourceInfo(metrics.getAvailableResources());
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

      for (RMNode rmNode : rmNodes){
        int amCount = nodeToAMRunningCount.getOrDefault(
            rmNode.getNodeID().toString(), 0);
        int runningAppCount = nodeToAppsRunningCount.getOrDefault(
            rmNode.getNodeID().toString(), 0);
        boolean recommendFlag = true;
        if (amCount != 0 || runningAppCount != 0 ||
            rmNode.getState() == NodeState.DECOMMISSIONED ||
            rmNode.getState() == NodeState.SHUTDOWN) {
          recommendFlag = false;
        }
        int deTimeout = nodeToDecommissioningTimeoutSecs.getOrDefault(rmNode.getNodeID().toString(),
            -1);
        if (recommendFlag) {
          DecommissionCandidateNodeInfo dcni = new DecommissionCandidateNodeInfo(
              amCount,
              runningAppCount,
              deTimeout,
              rmNode.getState(),
              rmNode.getNodeID().toString(),
              recommendFlag
          );
          decommissionCandidates.add(dcni);
        }
      } // end for
      // if no scale down requirement, check scale up
      if (pendingAppCount > 0 ||
          pendingContainersCount > 0) {
        // given existing node types, found the maximum count of instance
        // that can serve the pending resource. Generally, the more instance,
        // the more opportunity to scale down
        ResourceCalculator rc = new DominantResourceCalculator();
        Map<Resource, Integer> containerAskToCount = metrics.getContainerAskToCount();
        NodeInstanceType[] allTypes = NodeInstanceType.getAllNodeInstanceType();
        recommendNewInstances(containerAskToCount, newNMCandidates, allTypes, rc);
      }// end if


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

  public static void recommendNewInstances(Map<Resource, Integer> pendingContainers,
      NewNMCandidates newNMCandidates, NodeInstanceType[] allTypes, ResourceCalculator rc) {
    int[] suitableInstanceRet = null;
    StringBuilder tip = new StringBuilder();

    Iterator<Map.Entry<Resource, Integer>> it = pendingContainers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Resource, Integer> entry = it.next();
      // What if the existing new instance have headroom to allocate for some containers?
      // try allocate on existing nodes' headroom
      scheduleBasedOnRecommendedNewInstance(entry.getKey(), entry.getValue(),
          newNMCandidates, entry, rc);
      if (entry.getValue() == 0) {
        // this means we can allocate on existing new instance's headroom
        continue;
      }
      suitableInstanceRet = NodeInstanceType.getSuitableInstanceType(
          entry.getKey(), allTypes, rc);
      int ti = suitableInstanceRet[0];
      if (ti == -1) {
        tip.append(String.format(
            "No capable instance type for container resource: %s, count: %d",
            entry.getKey(), entry.getValue()));
      } else {
        Resource containerResource = entry.getKey();
        int containerCount = entry.getValue();
        NodeInstanceType t = allTypes[ti];
        int buckets = suitableInstanceRet[1];
        int instanceCount = (int)Math.ceil((double)containerCount/(double)buckets);
        Resource planToUseResourceInThisNodeType = Resources.multiplyAndRoundUp(containerResource, containerCount);
        newNMCandidates.add(t, instanceCount, planToUseResourceInThisNodeType);
        newNMCandidates.setRecommendActionTime("Now");
      }
    }
  }

  public static void scheduleBasedOnRecommendedNewInstance(Resource containerRes,
      int count, NewNMCandidates newNMCandidates,
      Map.Entry<Resource, Integer> entry, ResourceCalculator rc) {
    for (NewSingleTypeNMCandidate singleTypeNMCandidate : newNMCandidates.getNewNMCandidates()) {
      Resource headroom = singleTypeNMCandidate.getPlanRemaining().getResource();
      Resource headroomInEveryNode = rc.divideAndCeil(headroom, singleTypeNMCandidate.getCount());
      long bucketsInExistingOneNode = rc.computeAvailableContainers(headroomInEveryNode, containerRes);
      if (bucketsInExistingOneNode > 0) {
        int prev = count;
        // we can allocate #buckets such container in existing one node
        count -= bucketsInExistingOneNode;
        if (count < 0) {
          count = 0;
        }
        entry.setValue(count);
        // update existing node's headroom
        singleTypeNMCandidate.addPlanToUse(
            Resources.multiplyAndRoundUp(containerRes, prev));
      } else {
        return;
      }
    }
  }

}
