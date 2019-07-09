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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * <p>
 * This class has the following functionality:
 *
 * <p>
 * ResourceUsageMultiNodeLookupPolicy holds sorted nodes list based on the
 * resource usage of nodes at given time.
 * </p>
 */
public class ResourceUsageMultiNodeLookupPolicy<N extends SchedulerNode>
    implements MultiNodeLookupPolicy<N> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceUsageMultiNodeLookupPolicy.class);

  protected Map<String, Set<N>> nodesPerPartition = new ConcurrentHashMap<>();
  protected Map<String, Resource> partitionTotalClusterResource =
      new HashMap<>();
  protected Map<String, Float> nodeToAMContainerPercentage = new HashMap<>();

  protected Map<String, Float> nodeToScore = new HashMap<>();

  public enum ScoreFactor {

    NODE_RESOURCE_USAGE_FACTOR("node-resource-usage"),
    NODE_AM_PERCENTAGE_KEY("am-container");

    private final String name;

    ScoreFactor(String n){this.name = n;}

    public String getName() {
      return name;
    }
  }

  public static float[] scoreFactorWeights =
      new float[ScoreFactor.values().length];

  protected Comparator<N> comparator;

  public ResourceUsageMultiNodeLookupPolicy() {
    setScoreFactorWeights();
    this.comparator = new Comparator<N>() {
      @Override
      public int compare(N o1, N o2) {
        float score1 = nodeToScore.get(o1.getNodeID().toString());
        float score2 = nodeToScore.get(o2.getNodeID().toString());
        float allocatedDiff = score2 - score1;
        if (allocatedDiff == 0) {
          return o1.getNodeID().compareTo(o2.getNodeID());
        }
        return allocatedDiff > 0 ? 1 : -1;
      }
    };
  }

  // Set weights to prefer some factor.
  // The weights will be normalized to a percentage
  private void setScoreFactorWeights() {
    scoreFactorWeights[ScoreFactor.NODE_RESOURCE_USAGE_FACTOR.ordinal()] = 1;
    scoreFactorWeights[ScoreFactor.NODE_AM_PERCENTAGE_KEY.ordinal()] = 2;
    float sumWeights = 0;
    for (int i = 0; i < scoreFactorWeights.length; i++) {
      sumWeights += scoreFactorWeights[i];
    }
    // each weight value will be 0 to 1
    for (int i = 0; i < scoreFactorWeights.length; i++) {
      scoreFactorWeights[i] /= sumWeights;
    }
  }

  private float calculateScore(N node) {
    LOG.debug("{}'s used resource is {}", node.getNodeID().toString(), node.getAllocatedResource());
    float nodeResourceScore = scoreNodeResourceUsage(node.getAllocatedResource(),
        partitionTotalClusterResource.get(node.getPartition()));
    float amScore = scoreNodeAMStatics(node);
    LOG.debug("{}'s am score is {}", node.getNodeID().toString(), amScore);
    LOG.debug("{}'s resource usage score is {}", node.getNodeID().toString(), nodeResourceScore);
    LOG.debug("The weights array is {}", scoreFactorWeights);
    int nodeUsageIndexInWeights = ScoreFactor.NODE_RESOURCE_USAGE_FACTOR.ordinal();
    int amPercentageIndexInWeights = ScoreFactor.NODE_RESOURCE_USAGE_FACTOR.ordinal();
    float score = nodeResourceScore * scoreFactorWeights[nodeUsageIndexInWeights] +
        amScore * scoreFactorWeights[amPercentageIndexInWeights];
    LOG.debug("{}'s final score is {}", node.getNodeID().toString(), score);
    return score;
  }

  private float scoreNodeResourceUsage(Resource r, Resource base) {
    float raw = (float)r.getMemorySize()/(float)base.getMemorySize() +
        (float)r.getVirtualCores()/(float)base.getVirtualCores();
    // The score ranges from 0 to 2, so divided by 2 to make it 0 to 1
    return raw/2;
  }

  // The score ranges from 0 to 1
  private float scoreNodeAMStatics(N node) {
    return nodeToAMContainerPercentage.getOrDefault(node.getNodeID().toString(),
        (float)0);
  }

  @Override
  public Iterator<N> getPreferredNodeIterator(Collection<N> nodes,
      String partition) {
    return getNodesPerPartition(partition).iterator();
  }

  @Override
  public void addAndRefreshNodesSet(Collection<N> nodes,
      String partition) {
    Resource totalResourceInPartition = Resource.newInstance(0, 0);
    int totalAMsInPartition = 0;
    for (N node : nodes) {
      float amCountInNode = 0;
      Resources.addTo(totalResourceInPartition, node.getTotalResource());
      for (RMContainer rmContainer: node.getCopiedListOfRunningContainers()) {
        if (rmContainer.isAMContainer()) {
          amCountInNode++;
          totalAMsInPartition++;
        }
      }
      nodeToAMContainerPercentage.put(node.getNodeID().toString(),
          amCountInNode);
    }
    partitionTotalClusterResource.put(partition, totalResourceInPartition);
    // Update the value from am count to am percentage
    if (totalAMsInPartition != 0) {
      Iterator<Map.Entry<String, Float>> it =
          nodeToAMContainerPercentage.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, Float> element = it.next();
        float amCount = element.getValue();
        element.setValue(amCount / totalAMsInPartition);
      }
    }
    // Compute node's score
    for (N node : nodes) {
      nodeToScore.put(node.getNodeID().toString(), calculateScore(node));
    }

    Set<N> nodeList = new ConcurrentSkipListSet<N>(comparator);
    nodeList.addAll(nodes);
    /**
     * Assume nodes are uniform.
     * Pick three partition points as 5, 3, 2 percentage of the list
     * 50% should be high usage, 30% should be medium, 20% should be low
     * shuffle each partition
     * */
    //TODO Zhankun:

    nodesPerPartition.put(partition, Collections.unmodifiableSet(nodeList));
  }

  @Override
  public Set<N> getNodesPerPartition(String partition) {
    return nodesPerPartition.getOrDefault(partition, Collections.emptySet());
  }
}
