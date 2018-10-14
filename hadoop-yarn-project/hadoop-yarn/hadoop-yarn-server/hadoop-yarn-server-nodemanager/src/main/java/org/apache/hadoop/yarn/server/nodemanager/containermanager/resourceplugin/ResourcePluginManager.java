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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceConstants;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRegisterRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.DeviceLocalScheduler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework.DevicePluginAdapter;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.fpga.FpgaResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu.GpuResourcePlugin;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.FPGA_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.GPU_URI;

/**
 * Manages {@link ResourcePlugin} configured on this NodeManager.
 */
public class ResourcePluginManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(ResourcePluginManager.class);
  private static final Set<String> SUPPORTED_RESOURCE_PLUGINS = ImmutableSet.of(
      GPU_URI, FPGA_URI);

  private Map<String, ResourcePlugin> configuredPlugins =
          Collections.emptyMap();

  private DeviceLocalScheduler deviceLocalScheduler = null;

  public synchronized void initialize(Context context)
      throws YarnException {
    Configuration conf = context.getConf();
    Map<String, ResourcePlugin> pluginMap = new HashMap<>();

    String[] plugins = conf.getStrings(YarnConfiguration.NM_RESOURCE_PLUGINS);
    if (plugins != null) {
      // Initialize each plugins
      for (String resourceName : plugins) {
        resourceName = resourceName.trim();
        if (!SUPPORTED_RESOURCE_PLUGINS.contains(resourceName)) {
          String msg =
              "Trying to initialize resource plugin with name=" + resourceName
                  + ", it is not supported, list of supported plugins:"
                  + StringUtils.join(",",
                  SUPPORTED_RESOURCE_PLUGINS);
          LOG.error(msg);
          throw new YarnException(msg);
        }

        if (pluginMap.containsKey(resourceName)) {
          // Duplicated items, ignore ...
          continue;
        }

        ResourcePlugin plugin = null;
        if (resourceName.equals(GPU_URI)) {
          plugin = new GpuResourcePlugin();
        }

        if (resourceName.equals(FPGA_URI)) {
          plugin = new FpgaResourcePlugin();
        }

        if (plugin == null) {
          throw new YarnException(
              "This shouldn't happen, plugin=" + resourceName
                  + " should be loaded and initialized");
        }
        plugin.initialize(context);
        pluginMap.put(resourceName, plugin);
      }
    }
    // Try to load extended device plugins
    initializeExtendedDevicePlugins(context, conf, pluginMap);
    configuredPlugins = Collections.unmodifiableMap(pluginMap);
  }

  public void initializeExtendedDevicePlugins(Context context,
      Configuration configuration,
      Map<String, ResourcePlugin> pluginMap) {
    boolean enable = configuration.getBoolean(
        YarnConfiguration.NM_RESOURCE_PLUGINS_ENABLE_EXTENDED_DEVICE,
        YarnConfiguration.DEFAULT_NM_RESOURCE_PLUGINS_ENABLE_EXTENDED_DEVICE);
    if (enable) {
      LOG.info("New device framework enabled, trying to load the vendor plugins");
      deviceLocalScheduler = new DeviceLocalScheduler(context);
      String[] pluginClassNames = configuration.getStrings(
          YarnConfiguration.NM_RESOURCE_PLUGINS_EXTENDED);
      if (pluginClassNames != null) {
        for (String pluginClassName : pluginClassNames) {
          try {
            Class<?> pluginClazz = Class.forName(pluginClassName);
            if (DevicePlugin.class.isAssignableFrom(pluginClazz)) {
              DevicePlugin dpInstance = (DevicePlugin) ReflectionUtils.newInstance(pluginClazz,
                  configuration);
              // Try to register plugin
              // TODO: handle the plugin method timeout issue
              DeviceRegisterRequest request = dpInstance.register();
              if (request == null) {
                LOG.error("Class: " + pluginClassName +
                    " register() method return null. Expected a resource name");
                continue;
              }
              // Check version for compatibility
              String version = request.getVersion();
              if (!version.equals(DeviceConstants.version)) {
                LOG.error("Class: " + pluginClassName + " version: " + version +
                    " is not compatible. Expected: " + DeviceConstants.version);
              }

              String resourceName = request.getResourceName();
              if (resourceName == null) {
                LOG.error("Class: " + pluginClassName + " register invalid resource name: "+
                    resourceName);
                continue;
              }
              // check if someone has already registered this resource type name
              if (pluginMap.containsKey(resourceName)) {
                LOG.error(resourceName +
                    " has already been registered! Please change a resource type name");
                continue;
              }

              // check resource name is valid and configured in resource-types.xml
              if (!isValidAndConfiguredResourceName(resourceName)) {
                LOG.error(resourceName
                    + " is not configured inside "
                    + YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE +
                    " , please configure it to enable");
                continue;
              }

              DevicePluginAdapter pluginAdapter = new DevicePluginAdapter(this,
                  resourceName, dpInstance);
              LOG.info("Adapter of " + pluginClassName + " created. Initializing..");
              try {
                pluginAdapter.initialize(context);
              } catch (YarnException e) {
                LOG.error("Adapter of " + pluginClassName + " init failed!");
                e.printStackTrace();
                continue;
              }
              LOG.info("Adapter of " + pluginClassName + " init success!");
              // Store plugin as adapter instance
              pluginMap.put(request.getResourceName(), pluginAdapter);
            } else {
              throw new YarnRuntimeException("Class: " + pluginClassName
                  + " not instance of " + DevicePlugin.class.getCanonicalName());
            }
          } catch(ClassNotFoundException e) {
            throw new YarnRuntimeException("Could not instantiate extended vendor plugin: "
                + pluginClassName, e);
          }
        } // end for
      } // end if
    } else {
      LOG.info("New device framework is not enabled. If you want, set true to " +
          YarnConfiguration.NM_RESOURCE_PLUGINS_ENABLE_EXTENDED_DEVICE);
    }
  }

  // TODO: check resource name matching pattern
  private boolean isValidAndConfiguredResourceName(String resourceName) {
    // check pattern match
    // check configured
    Map<String, ResourceInformation> configuredResourceTypes =
        ResourceUtils.getResourceTypes();
    if (!configuredResourceTypes.containsKey(resourceName)) {
      return false;
    }
    return true;
  }

  public DeviceLocalScheduler getDeviceLocalScheduler() {
    return deviceLocalScheduler;
  }

  public synchronized void cleanup() throws YarnException {
    for (ResourcePlugin plugin : configuredPlugins.values()) {
      plugin.cleanup();
    }
  }

  /**
   * Get resource name (such as gpu/fpga) to plugin references.
   * @return read-only map of resource name to plugins.
   */
  public synchronized Map<String, ResourcePlugin> getNameToPlugins() {
    return configuredPlugins;
  }
}
