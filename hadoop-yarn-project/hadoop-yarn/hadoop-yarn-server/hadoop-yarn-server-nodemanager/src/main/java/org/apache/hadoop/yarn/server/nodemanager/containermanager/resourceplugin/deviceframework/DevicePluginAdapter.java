package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.deviceframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DevicePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.DockerCommandPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.NodeResourceUpdaterPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.NMResourceInfo;

import java.util.List;
import java.util.Set;


/**
 * The {@link DevicePluginAdapter} will adapt existing hooks into vendor plugin's logic.
 * It decouples the vendor plugin from YARN's device framework
 *
 * */
public class DevicePluginAdapter extends NodeResourceUpdaterPlugin
    implements ResourcePlugin, DockerCommandPlugin, ResourceHandler{
  final static Log LOG = LogFactory.getLog(DevicePluginAdapter.class);

  private ResourcePluginManager devicePluginManager;
  private String resourceName;
  private DevicePlugin devicePlugin;
  private DeviceLocalScheduler deviceLocalScheduler;
  private CGroupsHandler cGroupsHandler;
  private PrivilegedOperationExecutor privilegedOperationExecutor;

  public DevicePluginAdapter(ResourcePluginManager pluginManager, String name, DevicePlugin dp) {
    devicePluginManager = pluginManager;
    deviceLocalScheduler = pluginManager.getDeviceLocalScheduler();
    resourceName = name;
    devicePlugin = dp;
  }

  /**
   * Act as a {@link NodeResourceUpdaterPlugin} to update the {@link Resource}
   *
   * */
  @Override
  public void updateConfiguredResource(Resource res) throws YarnException {
    LOG.info(resourceName + " plugin update resource ");
    Set<Device> devices = devicePlugin.getAndWatch();
    if (devices == null) {
      LOG.warn(resourceName + " plugin failed to discover resource ( null value got)." );
      return;
    }
    res.setResourceValue(resourceName, devices.size());
  }

  /**
   * Act as a {@link ResourcePlugin}
   * */
  @Override
  public void initialize(Context context) throws YarnException {
    LOG.info(resourceName + " plugin adapter initialized");
    return;
  }

  @Override
  public ResourceHandler createResourceHandler(Context nmContext, CGroupsHandler cGroupsHandler,
      PrivilegedOperationExecutor privilegedOperationExecutor) {
    this.cGroupsHandler = cGroupsHandler;
    this.privilegedOperationExecutor = privilegedOperationExecutor;
    return this;
  }

  @Override
  public NodeResourceUpdaterPlugin getNodeResourceHandlerInstance() {
    return this;
  }

  @Override
  public void cleanup() throws YarnException {

  }

  @Override
  public DockerCommandPlugin getDockerCommandPluginInstance() {
    return this;
  }

  @Override
  public NMResourceInfo getNMResourceInfo() throws YarnException {
    return null;
  }

  /**
   * Act as a {@link DockerCommandPlugin} to hook the
   * {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.DockerLinuxContainerRuntime}
   * */
  @Override
  public void updateDockerRunCommand(DockerRunCommand dockerRunCommand, Container container)
      throws ContainerExecutionException {

  }

  @Override
  public DockerVolumeCommand getCreateDockerVolumeCommand(Container container)
      throws ContainerExecutionException {
    return null;
  }

  @Override
  public DockerVolumeCommand getCleanupDockerVolumesCommand(Container container)
      throws ContainerExecutionException {
    return null;
  }

  /**
   * Act as a {@link ResourceHandler}
   * */
  @Override
  public List<PrivilegedOperation> bootstrap(Configuration configuration) throws ResourceHandlerException {
    Set<Device> availableDevices = devicePlugin.getAndWatch();

    /**
     * We won't fail the NM if plugin returns invalid value here.
     * // TODO: we should update RM's resource count if something wrong
     * */
    if (availableDevices == null) {
      LOG.error("Bootstrap " + resourceName + " failed. Null value got from plugin's getAndWatch method");
      return null;
    }
    // Add device set. Here we trust the plugin's return value
    deviceLocalScheduler.addDeviceSet(resourceName, availableDevices);
    // Init cgroups
    this.cGroupsHandler.initializeCGroupController(
        CGroupsHandler.CGroupController.DEVICES);
    return null;
  }

  @Override
  public List<PrivilegedOperation> preStart(Container container)
      throws ResourceHandlerException {
    String containerIdStr = container.getContainerId().toString();
    DeviceLocalScheduler.DeviceAllocation allocation = deviceLocalScheduler.assignDevices(
        resourceName, container);
    /**
     * TODO: implement a general container-executor device module to do isolation
     * */
    return null;
  }

  @Override
  public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
      throws ResourceHandlerException {
    deviceLocalScheduler.recoverAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> updateContainer(Container container)
      throws ResourceHandlerException {
    return null;
  }

  @Override
  public List<PrivilegedOperation> postComplete(ContainerId containerId)
      throws ResourceHandlerException {
    deviceLocalScheduler.cleanupAssignedDevices(resourceName, containerId);
    return null;
  }

  @Override
  public List<PrivilegedOperation> teardown() throws ResourceHandlerException {
    return null;
  }
}
