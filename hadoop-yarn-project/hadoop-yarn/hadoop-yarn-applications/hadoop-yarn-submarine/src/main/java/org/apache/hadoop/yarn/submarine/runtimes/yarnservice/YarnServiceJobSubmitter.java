/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.AppAdminClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.service.api.ServiceApiConstants;
import org.apache.hadoop.yarn.service.api.records.Artifact;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.Resource;
import org.apache.hadoop.yarn.service.api.records.ResourceInformation;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.KerberosPrincipal;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.submarine.client.cli.param.Localization;
import org.apache.hadoop.yarn.submarine.client.cli.param.Quicklink;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.ClientContext;
import org.apache.hadoop.yarn.submarine.common.Envs;
import org.apache.hadoop.yarn.submarine.common.api.TaskType;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.common.fs.RemoteDirectoryManager;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes.EXIT_SUCCESS;
import static org.apache.hadoop.yarn.service.utils.ServiceApiUtil.jsonSerDeser;

/**
 * Submit a job to cluster
 */
public class YarnServiceJobSubmitter implements JobSubmitter {
  public static final String TENSORBOARD_QUICKLINK_LABEL = "Tensorboard";
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnServiceJobSubmitter.class);
  ClientContext clientContext;
  Service serviceSpec;
  private Set<Path> uploadedFiles = new HashSet<>();

  // Used by testing
  private Map<String, String> componentToLocalLaunchScriptPath =
      new HashMap<>();

  public YarnServiceJobSubmitter(ClientContext clientContext) {
    this.clientContext = clientContext;
  }

  private Resource getServiceResourceFromYarnResource(
      org.apache.hadoop.yarn.api.records.Resource yarnResource) {
    Resource serviceResource = new Resource();
    serviceResource.setCpus(yarnResource.getVirtualCores());
    serviceResource.setMemory(String.valueOf(yarnResource.getMemorySize()));

    Map<String, ResourceInformation> riMap = new HashMap<>();
    for (org.apache.hadoop.yarn.api.records.ResourceInformation ri : yarnResource
        .getAllResourcesListCopy()) {
      ResourceInformation serviceRi =
          new ResourceInformation();
      serviceRi.setValue(ri.getValue());
      serviceRi.setUnit(ri.getUnits());
      riMap.put(ri.getName(), serviceRi);
    }
    serviceResource.setResourceInformations(riMap);

    return serviceResource;
  }

  private String getValueOfEnvionment(String envar) {
    // extract value from "key=value" form
    if (envar == null || !envar.contains("=")) {
      return "";
    } else {
      return envar.substring(envar.indexOf("=") + 1);
    }
  }

  private boolean needHdfs(String content) {
    return content != null && content.contains("hdfs://");
  }

  private void addHdfsClassPathIfNeeded(RunJobParameters parameters,
      PrintWriter fw, Component comp) throws IOException {
    // Find envs to use HDFS
    String hdfsHome = null;
    String javaHome = null;

    boolean hadoopEnv = false;

    for (String envar : parameters.getEnvars()) {
      if (envar.startsWith("DOCKER_HADOOP_HDFS_HOME=")) {
        hdfsHome = getValueOfEnvionment(envar);
        hadoopEnv = true;
      } else if (envar.startsWith("DOCKER_JAVA_HOME=")) {
        javaHome = getValueOfEnvionment(envar);
      }
    }

    boolean lackingEnvs = false;

    if (needHdfs(parameters.getInputPath()) || needHdfs(
        parameters.getPSLaunchCmd()) || needHdfs(
        parameters.getWorkerLaunchCmd()) || hadoopEnv) {
      // HDFS is asked either in input or output, set LD_LIBRARY_PATH
      // and classpath
      if (hdfsHome != null) {
        // Unset HADOOP_HOME/HADOOP_YARN_HOME to make sure host machine's envs
        // won't pollute docker's env.
        fw.append("export HADOOP_HOME=\n");
        fw.append("export HADOOP_YARN_HOME=\n");
        fw.append("export HADOOP_HDFS_HOME=" + hdfsHome + "\n");
        fw.append("export HADOOP_COMMON_HOME=" + hdfsHome + "\n");
      } else{
        lackingEnvs = true;
      }

      // hadoop confs will be uploaded to HDFS and localized to container's
      // local folder, so here set $HADOOP_CONF_DIR to $WORK_DIR.
      fw.append("export HADOOP_CONF_DIR=$WORK_DIR\n");
      if (javaHome != null) {
        fw.append("export JAVA_HOME=" + javaHome + "\n");
        fw.append("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"
            + "$JAVA_HOME/lib/amd64/server\n");
      } else {
        lackingEnvs = true;
      }
      fw.append("export CLASSPATH=`$HADOOP_HDFS_HOME/bin/hadoop classpath --glob`\n");
    }

    if (lackingEnvs) {
      LOG.error("When hdfs is being used to read/write models/data. Following"
          + "envs are required: 1) DOCKER_HADOOP_HDFS_HOME=<HDFS_HOME inside"
          + "docker container> 2) DOCKER_JAVA_HOME=<JAVA_HOME inside docker"
          + "container>. You can use --env to pass these envars.");
      throw new IOException("Failed to detect HDFS-related environments.");
    }

    // Trying to upload core-site.xml and hdfs-site.xml
    Path stagingDir =
        clientContext.getRemoteDirectoryManager().getJobStagingArea(
            parameters.getName(), true);
    File coreSite = findFileOnClassPath("core-site.xml");
    File hdfsSite = findFileOnClassPath("hdfs-site.xml");
    if (coreSite == null || hdfsSite == null) {
      LOG.error("hdfs is being used, however we couldn't locate core-site.xml/"
          + "hdfs-site.xml from classpath, please double check you classpath"
          + "setting and make sure they're included.");
      throw new IOException(
          "Failed to locate core-site.xml / hdfs-site.xml from class path");
    }
    uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir,
        coreSite.getAbsolutePath(), "core-site.xml", comp);
    uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir,
        hdfsSite.getAbsolutePath(), "hdfs-site.xml", comp);

    // DEBUG
    if (SubmarineLogs.isVerbose()) {
      fw.append("echo \"CLASSPATH:$CLASSPATH\"\n");
      fw.append("echo \"HADOOP_CONF_DIR:$HADOOP_CONF_DIR\"\n");
      fw.append("echo \"HADOOP_TOKEN_FILE_LOCATION:$HADOOP_TOKEN_FILE_LOCATION\"\n");
      fw.append("echo \"JAVA_HOME:$JAVA_HOME\"\n");
      fw.append("echo \"LD_LIBRARY_PATH:$LD_LIBRARY_PATH\"\n");
      fw.append("echo \"HADOOP_HDFS_HOME:$HADOOP_HDFS_HOME\"\n");
    }
  }

  private void addCommonEnvironments(Component component, TaskType taskType) {
    Map<String, String> envs = component.getConfiguration().getEnv();
    envs.put(Envs.TASK_INDEX_ENV, ServiceApiConstants.COMPONENT_ID);
    envs.put(Envs.TASK_TYPE_ENV, taskType.name());
  }

  @VisibleForTesting
  protected String getUserName() {
    return System.getProperty("user.name");
  }

  private String getDNSDomain() {
    return clientContext.getYarnConfig().get("hadoop.registry.dns.domain-name");
  }

  /*
   * Generate a command launch script on local disk, returns patch to the script
   */
  private String generateCommandLaunchScript(RunJobParameters parameters,
      TaskType taskType, Component comp) throws IOException {
    File file = File.createTempFile(taskType.name() + "-launch-script", ".sh");
    Writer w = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
    PrintWriter pw = new PrintWriter(w);

    try {
      pw.append("#!/bin/bash\n");

      addHdfsClassPathIfNeeded(parameters, pw, comp);

      if (taskType.equals(TaskType.TENSORBOARD)) {
        String tbCommand =
            "export LC_ALL=C && tensorboard --logdir=" + parameters
                .getCheckpointPath();
        pw.append(tbCommand + "\n");
        LOG.info("Tensorboard command=" + tbCommand);
      } else{
        // When distributed training is required
        if (parameters.isDistributed()) {
          // Generated TF_CONFIG
          String tfConfigEnv = YarnServiceUtils.getTFConfigEnv(
              taskType.getComponentName(), parameters.getNumWorkers(),
              parameters.getNumPS(), parameters.getName(), getUserName(),
              getDNSDomain());
          pw.append("export TF_CONFIG=\"" + tfConfigEnv + "\"\n");
        }

        // Print launch command
        if (taskType.equals(TaskType.WORKER) || taskType.equals(
            TaskType.PRIMARY_WORKER)) {
          pw.append(parameters.getWorkerLaunchCmd() + '\n');

          if (SubmarineLogs.isVerbose()) {
            LOG.info(
                "Worker command =[" + parameters.getWorkerLaunchCmd() + "]");
          }
        } else if (taskType.equals(TaskType.PS)) {
          pw.append(parameters.getPSLaunchCmd() + '\n');

          if (SubmarineLogs.isVerbose()) {
            LOG.info("PS command =[" + parameters.getPSLaunchCmd() + "]");
          }
        }
      }
    } finally {
      pw.close();
    }
    return file.getAbsolutePath();
  }

  private String getScriptFileName(TaskType taskType) {
    return "run-" + taskType.name() + ".sh";
  }

  private File findFileOnClassPath(final String fileName) {
    final String classpath = System.getProperty("java.class.path");
    final String pathSeparator = System.getProperty("path.separator");
    final StringTokenizer tokenizer = new StringTokenizer(classpath,
        pathSeparator);

    while (tokenizer.hasMoreTokens()) {
      final String pathElement = tokenizer.nextToken();
      final File directoryOrJar = new File(pathElement);
      final File absoluteDirectoryOrJar = directoryOrJar.getAbsoluteFile();
      if (absoluteDirectoryOrJar.isFile()) {
        final File target = new File(absoluteDirectoryOrJar.getParent(),
            fileName);
        if (target.exists()) {
          return target;
        }
      } else{
        final File target = new File(directoryOrJar, fileName);
        if (target.exists()) {
          return target;
        }
      }
    }

    return null;
  }

  private void uploadToRemoteFileAndLocalizeToContainerWorkDir(Path stagingDir,
      String fileToUpload, String destFilename, Component comp,
      ConfigFile.TypeEnum type)
      throws IOException {
    Path uploadedFilePath = uploadToRemoteFile(stagingDir, fileToUpload);
    locateRemoteFileToContainerWorkDir(destFilename, comp, uploadedFilePath);
  }

  private void locateRemoteFileToContainerWorkDir(String destFilename,
      Component comp, Path uploadedFilePath) throws IOException {
    FileSystem fs = FileSystem.get(clientContext.getYarnConfig());

    FileStatus fileStatus = fs.getFileStatus(uploadedFilePath);
    LOG.info("Uploaded file path = " + fileStatus.getPath());

    // Set it to component's files list
    comp.getConfiguration().getFiles().add(new ConfigFile().srcFile(
        fileStatus.getPath().toUri().toString()).destFile(destFilename)
        .type(ConfigFile.TypeEnum.STATIC));
  }

  private Path uploadToRemoteFile(Path stagingDir, String fileToUpload) throws
      IOException {
    FileSystem fs = FileSystem.get(clientContext.getYarnConfig());
    // Upload to remote FS under staging area
    File localFile = new File(fileToUpload);
    if (!localFile.exists()) {
      throw new FileNotFoundException(
          "Trying to upload file=" + localFile.getAbsolutePath()
              + " to remote, but couldn't find local file.");
    }
    String filename = new File(fileToUpload).getName();

    Path uploadedFilePath = new Path(stagingDir, filename);
    if (!uploadedFiles.contains(uploadedFilePath)) {
      if (SubmarineLogs.isVerbose()) {
        LOG.info("Copying local file=" + fileToUpload + " to remote="
            + uploadedFilePath);
      }
      fs.copyFromLocalFile(new Path(fileToUpload), uploadedFilePath);
      uploadedFiles.add(uploadedFilePath);
    }
    return uploadedFilePath;
  }

  private void setPermission(Path destPath, FsPermission permission) throws
      IOException {
    FileSystem fs = FileSystem.get(clientContext.getYarnConfig());
    fs.setPermission(destPath, new FsPermission(permission));
  }

  private void handleLaunchCommand(RunJobParameters parameters,
      TaskType taskType, Component component) throws IOException {
    // Get staging area directory
    Path stagingDir =
        clientContext.getRemoteDirectoryManager().getJobStagingArea(
            parameters.getName(), true);

    // Generate script file in the local disk
    String localScriptFile = generateCommandLaunchScript(parameters, taskType,
        component);
    String destScriptFileName = getScriptFileName(taskType);
    uploadToRemoteFileAndLocalizeToContainerWorkDir(stagingDir, localScriptFile,
        destScriptFileName, component, ConfigFile.TypeEnum.STATIC);

    component.setLaunchCommand("./" + destScriptFileName);
    componentToLocalLaunchScriptPath.put(taskType.getComponentName(),
        localScriptFile);
  }

  private String getLastNameFromPath(String srcFileStr) {
    return new Path(srcFileStr).getName();
  }

  private String mayDownloadAndZipIt(String remoteDir, String zipFileName)
      throws IOException {
    String srcDir = remoteDir;
    String zipDirName = zipFileName;
    String zipDirPath = System.getProperty("java.io.tmpdir") + zipDirName;
    if (needHdfs(remoteDir)) {
      // Download them to temp dir
      boolean downloaded = clientContext.getRemoteDirectoryManager()
          .copyFilesFromHdfs(remoteDir, zipDirPath);
      if (!downloaded) {
        throw new IOException("Failed to download files from "
            + remoteDir);
      }
      LOG.info("Downloaded {} to {}", remoteDir, zipDirPath);
      srcDir = zipDirPath;
    }
    // zip a local dir
    return zipDir(srcDir, zipDirPath + ".zip");
  }

  private String zipDir(String srcDir, String dstFile) throws IOException {
    FileOutputStream fos = new FileOutputStream(dstFile);
    ZipOutputStream zos = new ZipOutputStream(fos);
    File srcFile = new File(srcDir);
    addDirToZip(zos, srcFile);
    // close the ZipOutputStream
    zos.close();
    LOG.info("Compressed {} to {}", srcDir, dstFile);
    return dstFile;
  }

  private void addDirToZip(ZipOutputStream zos, File srcFile) throws IOException {
    File[] files = srcFile.listFiles();
    if (null == files) {
      return;
    }
    FileInputStream fis = null;
    for (int i = 0; i < files.length; i++) {
      // if it's directory, add recursively
      if (files[i].isDirectory()) {
        addDirToZip(zos, files[i]);
        continue;
      }
      byte[] buffer = new byte[1024];
      try {
        fis = new FileInputStream(files[i]);
        zos.putNextEntry(new ZipEntry(files[i].getName()));
        int length;
        while ((length = fis.read(buffer)) > 0) {
          zos.write(buffer, 0, length);
        }
        zos.flush();
      } finally {
        if (fis != null) {
          fis.close();
        }
        zos.closeEntry();
      }
    }
  }

  private boolean isDir(String remoteUri) throws IOException {
    if (needHdfs(remoteUri)) {
      return clientContext.getRemoteDirectoryManager()
          .getFileSystem().getFileStatus(new Path(remoteUri)).isDirectory();
    } else {
      return new File(remoteUri).isDirectory();
    }
  }

  private void addWorkerComponent(Service service,
      RunJobParameters parameters, TaskType taskType) throws IOException {
    Component workerComponent = new Component();
    addCommonEnvironments(workerComponent, taskType);

    workerComponent.setName(taskType.getComponentName());

    if (taskType.equals(TaskType.PRIMARY_WORKER)) {
      workerComponent.setNumberOfContainers(1L);
    } else{
      workerComponent.setNumberOfContainers(
          (long) parameters.getNumWorkers() - 1);
    }

    if (parameters.getWorkerDockerImage() != null) {
      workerComponent.setArtifact(
          getDockerArtifact(parameters.getWorkerDockerImage()));
    }

    workerComponent.setResource(
        getServiceResourceFromYarnResource(parameters.getWorkerResource()));
    handleLaunchCommand(parameters, taskType, workerComponent);
    workerComponent.setRestartPolicy(Component.RestartPolicyEnum.NEVER);
    service.addComponent(workerComponent);
  }

  // Handle worker and primary_worker.
  private void addWorkerComponents(Service service, RunJobParameters parameters)
      throws IOException {
    addWorkerComponent(service, parameters, TaskType.PRIMARY_WORKER);

    if (parameters.getNumWorkers() > 1) {
      addWorkerComponent(service, parameters, TaskType.WORKER);
    }
  }

  private void appendToEnv(Service service, String key, String value,
      String delim) {
    Map<String, String> env = service.getConfiguration().getEnv();
    if (!env.containsKey(key)) {
      env.put(key, value);
    } else {
      if (!value.isEmpty()) {
        String existingValue = env.get(key);
        if (!existingValue.endsWith(delim)) {
          env.put(key, existingValue + delim + value);
        } else {
          env.put(key, existingValue + value);
        }
      }
    }
  }

  private void handleServiceEnvs(Service service, RunJobParameters parameters) {
    if (parameters.getEnvars() != null) {
      for (String envarPair : parameters.getEnvars()) {
        String key, value;
        if (envarPair.contains("=")) {
          int idx = envarPair.indexOf('=');
          key = envarPair.substring(0, idx);
          value = envarPair.substring(idx + 1);
        } else{
          // No "=" found so use the whole key
          key = envarPair;
          value = "";
        }
        appendToEnv(service, key, value, ":");
      }
    }

    // Append other configs like /etc/passwd, /etc/krb5.conf
    appendToEnv(service, "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS",
        "/etc/passwd:/etc/passwd:ro", ",");

    String authenication = clientContext.getYarnConfig().get(
        HADOOP_SECURITY_AUTHENTICATION);
    if (authenication != null && authenication.equals("kerberos")) {
      appendToEnv(service, "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS",
          "/etc/krb5.conf:/etc/krb5.conf:ro", ",");
    }
  }

  private Artifact getDockerArtifact(String dockerImageName) {
    return new Artifact().type(Artifact.TypeEnum.DOCKER).id(dockerImageName);
  }

  private void handleQuicklinks(RunJobParameters runJobParameters)
      throws IOException {
    List<Quicklink> quicklinks = runJobParameters.getQuicklinks();
    if (null != quicklinks && !quicklinks.isEmpty()) {
      for (Quicklink ql : quicklinks) {
        // Make sure it is a valid instance name
        String instanceName = ql.getComponentInstanceName();
        boolean found = false;

        for (Component comp : serviceSpec.getComponents()) {
          for (int i = 0; i < comp.getNumberOfContainers(); i++) {
            String possibleInstanceName = comp.getName() + "-" + i;
            if (possibleInstanceName.equals(instanceName)) {
              found = true;
              break;
            }
          }
        }

        if (!found) {
          throw new IOException(
              "Couldn't find a component instance = " + instanceName
                  + " while adding quicklink");
        }

        String link = ql.getProtocol() + YarnServiceUtils.getDNSName(
            serviceSpec.getName(), instanceName, getUserName(), getDNSDomain(),
            ql.getPort());
        YarnServiceUtils.addQuicklink(serviceSpec, ql.getLabel(), link);
      }
    }
  }

  private Service createServiceByParameters(RunJobParameters parameters)
      throws IOException {
    componentToLocalLaunchScriptPath.clear();
    serviceSpec = new Service();
    serviceSpec.setName(parameters.getName());
    serviceSpec.setVersion(String.valueOf(System.currentTimeMillis()));
    serviceSpec.setArtifact(getDockerArtifact(parameters.getDockerImageName()));
    handleKerberosPrincipal(parameters);

    handleServiceEnvs(serviceSpec, parameters);

    handleLocalizations(serviceSpec, parameters);

    if (parameters.getNumWorkers() > 0) {
      addWorkerComponents(serviceSpec, parameters);
    }

    if (parameters.getNumPS() > 0) {
      Component psComponent = new Component();
      psComponent.setName(TaskType.PS.getComponentName());
      addCommonEnvironments(psComponent, TaskType.PS);
      psComponent.setNumberOfContainers((long) parameters.getNumPS());
      psComponent.setRestartPolicy(Component.RestartPolicyEnum.NEVER);
      psComponent.setResource(
          getServiceResourceFromYarnResource(parameters.getPsResource()));

      // Override global docker image if needed.
      if (parameters.getPsDockerImage() != null) {
        psComponent.setArtifact(
            getDockerArtifact(parameters.getPsDockerImage()));
      }
      handleLaunchCommand(parameters, TaskType.PS, psComponent);
      serviceSpec.addComponent(psComponent);
    }

    if (parameters.isTensorboardEnabled()) {
      Component tbComponent = new Component();
      tbComponent.setName(TaskType.TENSORBOARD.getComponentName());
      addCommonEnvironments(tbComponent, TaskType.TENSORBOARD);
      tbComponent.setNumberOfContainers(1L);
      tbComponent.setRestartPolicy(Component.RestartPolicyEnum.NEVER);
      tbComponent.setResource(getServiceResourceFromYarnResource(
          parameters.getTensorboardResource()));
      if (parameters.getTensorboardDockerImage() != null) {
        tbComponent.setArtifact(
            getDockerArtifact(parameters.getTensorboardDockerImage()));
      }

      handleLaunchCommand(parameters, TaskType.TENSORBOARD, tbComponent);

      // Add tensorboard to quicklink
      String tensorboardLink = "http://" + YarnServiceUtils.getDNSName(
          parameters.getName(),
          TaskType.TENSORBOARD.getComponentName() + "-" + 0, getUserName(),
          getDNSDomain(), 6006);
      LOG.info("Link to tensorboard:" + tensorboardLink);
      serviceSpec.addComponent(tbComponent);

      YarnServiceUtils.addQuicklink(serviceSpec, TENSORBOARD_QUICKLINK_LABEL,
          tensorboardLink);
    }

    // After all components added, handle quicklinks
    handleQuicklinks(parameters);

    return serviceSpec;
  }

  /**
   * Localize dependencies for all containers.
   * If remoteUri is a local directory,
   * we'll compress it and upload to staging dir in HDFS
   * If remoteUri is a HDFS directory, we'll download, compress it
   * and upload to staging dir in HDFS
   * If localFilePath is ".", we'll use remote file/dir name
   * */
  private void handleLocalizations(Service serviceSpec,
      RunJobParameters parameters) throws IOException {
    // Handle localizations
    Path stagingDir =
        clientContext.getRemoteDirectoryManager().getJobStagingArea(
            parameters.getName(), true);
    List<Localization> locs = parameters.getLocalizations();
    String remoteUri;
    String containerLocalPath;
    for (Localization loc : locs) {
      remoteUri = loc.getRemoteUri();
      containerLocalPath = loc.getLocalPath();
      String srcFileStr = remoteUri;
      String localFileStr = containerLocalPath;
      String containerLocalFilePath = containerLocalPath;
      ConfigFile.TypeEnum destFileType = ConfigFile.TypeEnum.STATIC;
      // If remote is a dir, may download from hdfs and compress files
      if (clientContext.getRemoteDirectoryManager().isHdfsDir(remoteUri)) {
        destFileType = ConfigFile.TypeEnum.ARCHIVE;
        srcFileStr = mayDownloadAndZipIt(
            remoteUri, getLastNameFromPath(localFileStr));
      }
      if (containerLocalPath.equals(".")
          ||containerLocalPath.equals("./")) {
        localFileStr = getLastNameFromPath(srcFileStr);
        containerLocalFilePath = ApplicationConstants.Environment.PWD.$$()
            + "/" + getLastNameFromPath(localFileStr);
      }
      String hdfsDestUri = uploadToHdfs(stagingDir, srcFileStr);
      serviceSpec.getConfiguration().getFiles().add(new ConfigFile().srcFile(
          hdfsDestUri).destFile(getLastNameFromPath(localFileStr))
          .type(destFileType));
      // set mounts
      // mount path should be absolute
      if (containerLocalPath.startsWith("/")) {
        containerLocalFilePath = containerLocalPath;
      } else if (!containerLocalPath.startsWith(".")){
        // add container local dir path to be absolute one
        containerLocalFilePath = ApplicationConstants.Environment.PWD.$$()
            + "/" + containerLocalFilePath;
      }
      String mountStr = ApplicationConstants.Environment.PWD.$$()
          + "/" + getLastNameFromPath(localFileStr) + ":"
          + containerLocalFilePath + ":" + loc.getMountPermission();
      appendToEnv(serviceSpec, "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS",
          mountStr, ",");
    }
  }

  private String uploadToHdfs(Path stagingDir, String fileToUpload)
      throws IOException {
    RemoteDirectoryManager rdm = clientContext.getRemoteDirectoryManager();
    FileSystem fs = rdm.getFileSystem();
    Path uploadedFilePath;
    FileStatus fileStatus;
    // If it is a file path in HDFS, no upload
    if (needHdfs(fileToUpload)) {
      uploadedFilePath = new Path(fileToUpload);
      if (!rdm.existsHdfsFile(uploadedFilePath)) {
        throw new FileNotFoundException(
            "File " + fileToUpload
                + " seems a HDFS file, but doesn't exists.");
      }
      fileStatus = rdm.getHdfsFileStatus(uploadedFilePath);
      LOG.info("Remote File already in HDFS, no need to upload. "
          + fileStatus.getPath());
    } else {
      // Upload to remote FS under staging area
      File localFile = new File(fileToUpload);
      if (!localFile.exists()) {
        throw new FileNotFoundException(
            "Trying to upload file=" + localFile.getAbsolutePath()
                + " to remote, but couldn't find local file.");
      }
      String filename = new File(fileToUpload).getName();

      uploadedFilePath = new Path(stagingDir, filename);
      if (!uploadedFiles.contains(uploadedFilePath)) {
        if (SubmarineLogs.isVerbose()) {
          LOG.info("Copying local file=" + fileToUpload + " to remote="
              + uploadedFilePath);
        }
        fs.copyFromLocalFile(new Path(fileToUpload), uploadedFilePath);
        uploadedFiles.add(uploadedFilePath);
      }

      fileStatus = fs.getFileStatus(uploadedFilePath);
      LOG.info("Uploaded file path = " + fileStatus.getPath());
    }
    return fileStatus.getPath().toUri().toString();
  }

  private String generateServiceSpecFile(Service service) throws IOException {
    File serviceSpecFile = File.createTempFile(service.getName(), ".json");
    String buffer = jsonSerDeser.toJson(service);
    Writer w = new OutputStreamWriter(new FileOutputStream(serviceSpecFile),
        "UTF-8");
    PrintWriter pw = new PrintWriter(w);
    try {
      pw.append(buffer);
    } finally {
      pw.close();
    }
    return serviceSpecFile.getAbsolutePath();
  }

  private void handleKerberosPrincipal(RunJobParameters parameters) throws
      IOException {
    if(StringUtils.isNotBlank(parameters.getKeytab()) && StringUtils
        .isNotBlank(parameters.getPrincipal())) {
      String keytab = parameters.getKeytab();
      String principal = parameters.getPrincipal();
      if(parameters.isDistributeKeytab()) {
        Path stagingDir =
            clientContext.getRemoteDirectoryManager().getJobStagingArea(
                parameters.getName(), true);
        Path remoteKeytabPath = uploadToRemoteFile(stagingDir, keytab);
        //only the owner has read access
        setPermission(remoteKeytabPath,
            FsPermission.createImmutable((short)Integer.parseInt("400", 8)));
        serviceSpec.setKerberosPrincipal(new KerberosPrincipal().keytab(
            remoteKeytabPath.toString()).principalName(principal));
      } else {
        if(!keytab.startsWith("file")) {
          keytab = "file://" + keytab;
        }
        serviceSpec.setKerberosPrincipal(new KerberosPrincipal().keytab(
            keytab).principalName(principal));
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ApplicationId submitJob(RunJobParameters parameters)
      throws IOException, YarnException {
    createServiceByParameters(parameters);
    String serviceSpecFile = generateServiceSpecFile(serviceSpec);

    AppAdminClient appAdminClient = YarnServiceUtils.createServiceClient(
        clientContext.getYarnConfig());
    int code = appAdminClient.actionLaunch(serviceSpecFile,
        serviceSpec.getName(), null, null);
    if(code != EXIT_SUCCESS) {
      throw new YarnException("Fail to launch application with exit code:" +
          code);
    }

    String appStatus=appAdminClient.getStatusString(serviceSpec.getName());
    Service app=ServiceApiUtil.jsonSerDeser.fromJson(appStatus);
    if(app.getId() == null) {
      throw new YarnException("Can't get application id for Service " +
          serviceSpec.getName());
    }
    ApplicationId appid = ApplicationId.fromString(app.getId());
    appAdminClient.stop();
    return appid;
  }



  @VisibleForTesting
  public Service getServiceSpec() {
    return serviceSpec;
  }

  @VisibleForTesting
  public Map<String, String> getComponentToLocalLaunchScriptPath() {
    return componentToLocalLaunchScriptPath;
  }
}
