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

package org.apache.hadoop.yarn.util.resource;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Helper class to read the resource-types to be supported by the system.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ResourceUtils {

  public static final String UNITS = ".units";
  public static final String TYPE = ".type";
  public static final String MINIMUM_ALLOCATION = ".minimum-allocation";
  public static final String MAXIMUM_ALLOCATION = ".maximum-allocation";

  private static final String MEMORY = ResourceInformation.MEMORY_MB.getName();
  private static final String VCORES = ResourceInformation.VCORES.getName();

  private static final Set<String> DISALLOWED_NAMES = new HashSet<>();
  static {
    DISALLOWED_NAMES.add("memory");
    DISALLOWED_NAMES.add(MEMORY);
    DISALLOWED_NAMES.add(VCORES);
  }

  private static volatile Object lock;
  private static Map<String, ResourceInformation> readOnlyResources;
  private static volatile Object nodeLock;
  private static Map<String, ResourceInformation> readOnlyNodeResources;


  static final Log LOG = LogFactory.getLog(ResourceUtils.class);

  private ResourceUtils() {
  }

  private static void checkMandatatoryResources(
      Map<String, ResourceInformation> resourceInformationMap)
      throws YarnRuntimeException {
    if (resourceInformationMap.containsKey(MEMORY)) {
      ResourceInformation memInfo = resourceInformationMap.get(MEMORY);
      String memUnits = ResourceInformation.MEMORY_MB.getUnits();
      ResourceTypes memType = ResourceInformation.MEMORY_MB.getResourceType();
      if (!memInfo.getUnits().equals(memUnits) || !memInfo.getResourceType()
          .equals(memType)) {
        throw new YarnRuntimeException(
            "Attempt to re-define mandatory resource 'memory-mb'. It can only"
                + " be of type 'COUNTABLE' and have units 'Mi'.");
      }
    }

    if (resourceInformationMap.containsKey(VCORES)) {
      ResourceInformation vcoreInfo = resourceInformationMap.get(VCORES);
      String vcoreUnits = ResourceInformation.VCORES.getUnits();
      ResourceTypes vcoreType = ResourceInformation.VCORES.getResourceType();
      if (!vcoreInfo.getUnits().equals(vcoreUnits) || !vcoreInfo
          .getResourceType().equals(vcoreType)) {
        throw new YarnRuntimeException(
            "Attempt to re-define mandatory resource 'vcores'. It can only be"
                + " of type 'COUNTABLE' and have units ''(no units).");
      }
    }
  }

  private static void addManadtoryResources(
      Map<String, ResourceInformation> res) {
    ResourceInformation ri;
    if (!res.containsKey(MEMORY)) {
      LOG.info("Adding resource type - name = " + MEMORY + ", units = "
          + ResourceInformation.MEMORY_MB.getUnits() + ", type = "
          + ResourceTypes.COUNTABLE);
      ri = ResourceInformation
          .newInstance(MEMORY,
              ResourceInformation.MEMORY_MB.getUnits());
      res.put(MEMORY, ri);
    }
    if (!res.containsKey(VCORES)) {
      LOG.info("Adding resource type - name = " + VCORES + ", units = , type = "
          + ResourceTypes.COUNTABLE);
      ri =
          ResourceInformation.newInstance(VCORES);
      res.put(VCORES, ri);
    }
  }

  private static void setMinimumAllocationForMandatoryResources(
      Map<String, ResourceInformation> res, Configuration conf) {
    String[][] resourceTypesKeys =
        {
          { ResourceInformation.MEMORY_MB.getName(),
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            String.valueOf(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB),
            ResourceInformation.MEMORY_MB.getName()
          },
          { ResourceInformation.VCORES.getName(),
            YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            String.valueOf(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES),
            ResourceInformation.VCORES.getName()
          }
        };
    for (String[] arr : resourceTypesKeys) {
      String resourceTypesKey =
          YarnConfiguration.RESOURCE_TYPES + "." + arr[0] + MINIMUM_ALLOCATION;
      long minimumResourceTypes = conf.getLong(resourceTypesKey, -1);
      long minimumConf = conf.getLong(arr[1], -1);
      long minimum;
      if (minimumResourceTypes != -1) {
        minimum = minimumResourceTypes;
        if (minimumConf != -1) {
          LOG.warn("Using minimum allocation for memory specified in "
              + "resource-types config file with key "
              + minimumResourceTypes + ", ignoring minimum specified using "
              + arr[1]);
        }
      } else {
        minimum = conf.getLong(arr[1], Long.parseLong(arr[2]));
      }
      ResourceInformation ri = res.get(arr[3]);
      ri.setMinimumAllocation(minimum);
    }
  }

  private static void setMaximumAllocationForMandatoryResources(
      Map<String, ResourceInformation> res, Configuration conf) {
    String[][] resourceTypesKeys =
        {
          {
            ResourceInformation.MEMORY_MB.getName(),
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            String.valueOf(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB),
            ResourceInformation.MEMORY_MB.getName()
          },
          {
            ResourceInformation.VCORES.getName(),
            YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            String.valueOf(
              YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES),
             ResourceInformation.VCORES.getName()
          }
        };
    for (String[] arr : resourceTypesKeys) {
      String resourceTypesKey =
          YarnConfiguration.RESOURCE_TYPES + "." + arr[0] + MAXIMUM_ALLOCATION;
      long maximumResourceTypes = conf.getLong(resourceTypesKey, -1);
      long maximumConf = conf.getLong(arr[1], -1);
      long maximum;
      if (maximumResourceTypes != -1) {
        maximum = maximumResourceTypes;
        if (maximumConf != -1) {
          LOG.warn("Using maximum allocation for memory specified in "
              + "resource-types config file with key "
              + maximumResourceTypes + ", ignoring maximum specified using "
              + arr[1]);
        }
      } else {
        maximum = conf.getLong(arr[1], Long.parseLong(arr[2]));
      }
      ResourceInformation ri = res.get(arr[3]);
      ri.setMaximumAllocation(maximum);
    }
  }

  @VisibleForTesting
  static void initializeResourcesMap(Configuration conf,
      Map<String, ResourceInformation> resourceInformationMap) {

    String[] resourceNames = conf.getStrings(YarnConfiguration.RESOURCE_TYPES);

    if (resourceNames != null && resourceNames.length != 0) {
      for (String resourceName : resourceNames) {
        String resourceUnits = conf.get(
            YarnConfiguration.RESOURCE_TYPES + "." + resourceName + UNITS, "");
        String resourceTypeName = conf.get(
            YarnConfiguration.RESOURCE_TYPES + "." + resourceName + TYPE,
            ResourceTypes.COUNTABLE.toString());
        Long minimumAllocation = conf.getLong(
            YarnConfiguration.RESOURCE_TYPES + "." + resourceName
                + MINIMUM_ALLOCATION, 0L);
        Long maximumAllocation = conf.getLong(
            YarnConfiguration.RESOURCE_TYPES + "." + resourceName
                + MAXIMUM_ALLOCATION, Long.MAX_VALUE);
        if (resourceName == null || resourceName.isEmpty()
            || resourceUnits == null || resourceTypeName == null) {
          throw new YarnRuntimeException(
              "Incomplete configuration for resource type '" + resourceName
                  + "'. One of name, units or type is configured incorrectly.");
        }
        if (DISALLOWED_NAMES.contains(resourceName)) {
          throw new YarnRuntimeException(
              "Resource type cannot be named '" + resourceName
                  + "'. That name is disallowed.");
        }
        ResourceTypes resourceType = ResourceTypes.valueOf(resourceTypeName);
        LOG.info("Adding resource type - name = " + resourceName + ", units = "
            + resourceUnits + ", type = " + resourceTypeName);
        if (resourceInformationMap.containsKey(resourceName)) {
          throw new YarnRuntimeException(
              "Error in config, key '" + resourceName + "' specified twice");
        }
        resourceInformationMap.put(resourceName, ResourceInformation
            .newInstance(resourceName, resourceUnits, 0L, resourceType,
                minimumAllocation, maximumAllocation));
      }
    }
    checkMandatatoryResources(resourceInformationMap);
    addManadtoryResources(resourceInformationMap);
    setMinimumAllocationForMandatoryResources(resourceInformationMap, conf);
    setMaximumAllocationForMandatoryResources(resourceInformationMap, conf);
    readOnlyResources = Collections.unmodifiableMap(resourceInformationMap);
  }

  /**
   * Get the resource types to be supported by the system.
   * @return A map of the resource name to a ResouceInformation object
   *         which contains details such as the unit.
   */
  public static Map<String, ResourceInformation> getResourceTypes() {
    return getResourceTypes(null,
        YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE);
  }

  private static Map<String, ResourceInformation> getResourceTypes(
      Configuration conf) {
    return getResourceTypes(conf,
        YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE);
  }

  private static Map<String, ResourceInformation> getResourceTypes(
      Configuration conf, String resourceFile) {
    if (lock == null) {
      synchronized (ResourceUtils.class) {
        if (lock == null) {
          synchronized (ResourceUtils.class) {
            Map<String, ResourceInformation> resources = new HashMap<>();
            if (conf == null) {
              conf = new YarnConfiguration();
            }
            try {
              addResourcesFileToConf(resourceFile, conf);
              LOG.debug("Found " + resourceFile + ", adding to configuration");
              initializeResourcesMap(conf, resources);
              lock = new Object();
            } catch (FileNotFoundException fe) {
              LOG.info("Unable to find '" + resourceFile
                  + "'. Falling back to memory and vcores as resources", fe);
              initializeResourcesMap(conf, resources);
              lock = new Object();
            }
          }
        }
      }
    }
    return readOnlyResources;
  }

  private static InputStream getConfInputStream(String resourceFile,
      Configuration conf) throws IOException, YarnException {

    ConfigurationProvider provider =
        ConfigurationProviderFactory.getConfigurationProvider(conf);
    try {
      provider.init(conf);
    } catch (Exception e) {
      throw new IOException(e);
    }

    InputStream ris = provider.getConfigurationInputStream(conf, resourceFile);
    if (ris == null) {
      if (conf.getResource(resourceFile) == null) {
        throw new FileNotFoundException("Unable to find " + resourceFile);
      }
      throw new IOException(
          "Unable to open resource types file '" + resourceFile
              + "'. Using provider " + provider);
    }
    return ris;
  }

  private static void addResourcesFileToConf(String resourceFile,
      Configuration conf) throws FileNotFoundException {
    try {
      InputStream ris = getConfInputStream(resourceFile, conf);
      LOG.debug("Found " + resourceFile + ", adding to configuration");
      conf.addResource(ris);
    } catch (FileNotFoundException fe) {
      throw fe;
    } catch (IOException ie) {
      LOG.fatal("Exception trying to read resource types configuration '"
          + resourceFile + "'.", ie);
      throw new YarnRuntimeException(ie);
    } catch (YarnException ye) {
      LOG.fatal("YARN Exception trying to read resource types configuration '"
          + resourceFile + "'.", ye);
      throw new YarnRuntimeException(ye);
    }
  }

  @VisibleForTesting
  static void resetResourceTypes() {
    lock = null;
  }

  @VisibleForTesting
  public static void resetResourceTypes(Configuration conf) {
    lock = null;
    getResourceTypes(conf);
  }

  public static String getUnits(String resourceValue) {
    String units;
    for (int i = 0; i < resourceValue.length(); i++) {
      if (Character.isAlphabetic(resourceValue.charAt(i))) {
        units = resourceValue.substring(i);
        if (StringUtils.isAlpha(units)) {
          return units;
        }
      }
    }
    return "";
  }

  /**
   * Function to get the resources for a node. This function will look at the
   * file {@link YarnConfiguration#NODE_RESOURCES_CONFIGURATION_FILE} to
   * determine the node resources.
   *
   * @param conf configuration file
   * @return a map to resource name to the ResourceInformation object. The map
   * is guaranteed to have entries for memory and vcores
   */
  public static Map<String, ResourceInformation> getNodeResourceInformation(
      Configuration conf) {
    if (nodeLock == null) {
      synchronized (ResourceUtils.class) {
        if (nodeLock == null) {
          synchronized (ResourceUtils.class) {
            Map<String, ResourceInformation> nodeResources =
                initializeNodeResourceInformation(conf);
            addManadtoryResources(nodeResources);
            checkMandatatoryResources(nodeResources);
            readOnlyNodeResources = Collections.unmodifiableMap(nodeResources);
            nodeLock = new Object();
          }
        }
      }
    }
    return readOnlyNodeResources;
  }

  /**
   * Function to get the device allowed infomation. The value format should be comma separated majorNumber:minorNumber
   *
   * <property>
   *   <name>yarn.nodemanager.resource-types.MCP.allowed</name>
   *   <value>244:0,245:1</value>
   * </property>
   * @return a map of resource type and allowed value string
   * */
  public static Map<String, String> getResourceTypeAllowedValue(Configuration conf) {
    Map<String, String> allowedDevices = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key.startsWith(YarnConfiguration.NM_RESOURCES_PREFIX)) {
        String[] parts = key.split("\\.");
        LOG.info("Found allowed device resource entry " + key);
        if (parts.length == 5 && parts[4].equalsIgnoreCase("allowed")) {
          allowedDevices.put(parts[3], value);
        }
      }
    }
    return allowedDevices;
  }

  private static Map<String, ResourceInformation>
  initializeNodeResourceInformation(Configuration conf) {
    Map<String, ResourceInformation> nodeResources = new HashMap<>();
    try {
      addResourcesFileToConf(
          YarnConfiguration.NODE_RESOURCES_CONFIGURATION_FILE, conf);
      for (Map.Entry<String, String> entry : conf) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (key.startsWith(YarnConfiguration.NM_RESOURCES_PREFIX)) {
          addResourceInformation(key, value, nodeResources);
        }
      }
    } catch (FileNotFoundException fe) {
      LOG.info("Couldn't find node resources file");
    }
    return nodeResources;
  }

  private static void addResourceInformation(String prop, String value,
      Map<String, ResourceInformation> nodeResources) {
    String[] parts = prop.split("\\.");
    LOG.info("Found resource entry " + prop);
    if (parts.length == 4) {
      String resourceType = parts[3];
      if (!nodeResources.containsKey(resourceType)) {
        nodeResources
            .put(resourceType, ResourceInformation.newInstance(resourceType));
      }
      String units = getUnits(value);
      Long resourceValue =
          Long.valueOf(value.substring(0, value.length() - units.length()));
      nodeResources.get(resourceType).setValue(resourceValue);
      nodeResources.get(resourceType).setUnits(units);
      LOG.debug("Setting value for resource type " + resourceType + " to "
              + resourceValue + " with units " + units);
    }
  }

  @VisibleForTesting
  synchronized public static void resetNodeResources() {
    nodeLock = null;
  }

  public static Resource getResourceTypesMinimumAllocation() {
    Map<String, ResourceInformation> resourceTypes = getResourceTypes();
    Resource ret = Resource.newInstance(0, 0);
    for (Map.Entry<String, ResourceInformation> entry : resourceTypes
        .entrySet()) {
      String name = entry.getKey();
      if (name.equals(ResourceInformation.MEMORY_MB.getName())) {
        ret.setMemorySize(entry.getValue().getMinimumAllocation());
        continue;
      }
      if (name.equals(ResourceInformation.VCORES.getName())) {
        Long tmp = entry.getValue().getMinimumAllocation();
        if (tmp > Integer.MAX_VALUE) {
          tmp = (long) Integer.MAX_VALUE;
        }
        ret.setVirtualCores(tmp.intValue());
        continue;
      }
      ret.setResourceValue(name, entry.getValue().getMinimumAllocation());
    }
    return ret;
  }

  /**
   * Get a Resource object with for the maximum allocation possible.
   * @return a Resource object with the maximum allocation for the scheduler
   */
  public static Resource getResourceTypesMaximumAllocation() {
    Map<String, ResourceInformation> resourceTypes = getResourceTypes();
    Resource ret = Resource.newInstance(0, 0);
    for (Map.Entry<String, ResourceInformation> entry : resourceTypes
        .entrySet()) {
      String name = entry.getKey();
      if (name.equals(ResourceInformation.MEMORY_MB.getName())) {
        ret.setMemorySize(entry.getValue().getMaximumAllocation());
        continue;
      }
      if (name.equals(ResourceInformation.VCORES.getName())) {
        Long tmp = entry.getValue().getMaximumAllocation();
        if (tmp > Integer.MAX_VALUE) {
          tmp = (long) Integer.MAX_VALUE;
        }
        ret.setVirtualCores(tmp.intValue());
        continue;
      }
      ret.setResourceValue(name, entry.getValue().getMaximumAllocation());
    }
    return ret;
  }
}
