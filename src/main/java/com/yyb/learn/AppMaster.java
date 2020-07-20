package com.yyb.learn;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2019-11-28
 * @Time 17:52
 */
public class AppMaster {
    private static YarnConfiguration conf = new YarnConfiguration();
    private NMClient nmClient;
    private int containerCount = 1;
    private AMRMClientAsync amRMClient;
    private NMCallbackHandler containerListener;
    private NMClientAsync nmClientAsync;
    private int appMasterRpcPort = 0;
    private String appMasterTrackingUrl = "";
    private int numTotalContainers = 2;
    private int requestPriority = 0;
    private int containerMemory = 1024;
    private AtomicInteger numRequestedContainers = new AtomicInteger();
    private volatile boolean done;
    private List<Thread> launchThreads = new ArrayList<Thread>();
    private volatile boolean success;
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numFailedContainers = new AtomicInteger();

    private Map<String, String> shellEnv = new HashMap<String, String>();

    private String domainController;

    public static void main(String[] args) {
        System.out.println("AppMaster: Initializing");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            AppMaster app = new AppMaster();

            app.AM();

//            app.run();

            System.out.println("finished !");
            System.exit(0);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void run() throws Exception {
        for (int i = 0; i < 100; i++) {
            System.out.println("running ... ");
            Thread.sleep(1000);
        }
    }

    public void AM() throws Exception {
        Map<String, String> envs = System.getenv();

        //get containerId
        String containerIdString = envs.get(ApplicationConstants.Environment.CONTAINER_ID.name());
        if (containerIdString == null) {
            // container id should always be set in the env by the framework
            throw new IllegalArgumentException(
                    "ContainerId not set in the environment");
        }
        ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
        ApplicationAttemptId appAttemptId = containerId.getApplicationAttemptId();
        String attemptID = String.valueOf(appAttemptId.getAttemptId());

        System.out.println("attemptID: " + attemptID);


        // RM ResourceManager ;   Async Model
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        // NM NodeManager   ; Async Model
        containerListener = new NMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        // AM 交互
        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname,
                appMasterRpcPort,
                appMasterTrackingUrl);

        int maxMem = response.getMaximumResourceCapability().getMemory();
        System.out.println("Container maxMem: " + maxMem );

        for (int i = 0; i < numTotalContainers; ++i) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClient.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numTotalContainers);


        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }

        finish();



    }

    private void finish() {
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        nmClientAsync.stop();


        FinalApplicationStatus appStatus;
        String appMessage = null;
        success = true;
        if (numFailedContainers.get() == 0
                && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get()
                    + ", allocated=" + numAllocatedContainers.get()
                    + ", failed=" + numFailedContainers.get();
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage,
                    null);
        } catch (YarnException ex) {
            ex.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        done = true;
        amRMClient.stop();
    }

    private AMRMClient.ContainerRequest setupContainerAskForRM() {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(requestPriority);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(2);

        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null,
                pri);
        return request;
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            System.out.println("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                System.out.println("Got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                assert (containerStatus.getState() == ContainerState.COMPLETE);

                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                    }
                } else {
                    numCompletedContainers.incrementAndGet();
                    System.out.println("Container completed successfully."
                            + ", containerId="
                            + containerStatus.getContainerId());
                }
            }

            int askCount = numTotalContainers - numRequestedContainers.get();
            numRequestedContainers.addAndGet(askCount);

            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
                    amRMClient.addContainerRequest(containerAsk);
                }
            }

            if (numCompletedContainers.get() == numTotalContainers) {
                done = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            System.out.println("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                System.out.println("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode="
                        + allocatedContainer.getNodeId().getHost() + ":"
                        + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI="
                        + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());

                LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
                        allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> list) {

        }

        @Override
        public float getProgress() {
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        @Override
        public void onError(Throwable throwable) {
            done = true;
            amRMClient.stop();
        }
    }

    private class LaunchContainerRunnable implements Runnable {
        Container container;

        NMCallbackHandler containerListener;

        public LaunchContainerRunnable(Container lcontainer,
                                       NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        @Override
        public void run() {
            String containerId = container.getId().toString();

            System.out.println("Setting up container launch container for containerid="
                    + container.getId());
            ContainerLaunchContext ctx = Records
                    .newRecord(ContainerLaunchContext.class);

            ctx.setEnvironment(shellEnv);

            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            String applicationId = container.getId().getApplicationAttemptId()
                    .getApplicationId().toString();


            LocalResource appMasterJar = Records.newRecord(LocalResource.class);
            //staging
            String path = "hdfs://nameservice1/user/center/script/jars/";
            String myJar = "a.jar";
            String jarFile = path + myJar;
            Path jarPath = new Path(jarFile);
            try{
                FileStatus jarFileStatus = FileSystem.get(conf).getFileStatus(jarPath);
                appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
                appMasterJar.setSize(jarFileStatus.getLen());
                jarFileStatus.getLen();
                appMasterJar.setTimestamp(jarFileStatus.getModificationTime());
                appMasterJar.setType(LocalResourceType.FILE);
                appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
            }catch (Exception e){
                e.printStackTrace();
                return;
            }
            localResources.put("a.jar", appMasterJar);
            ctx.setLocalResources(localResources);
            Map<String, String> appMasterEnv = new HashMap();

            StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                    .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                    .append("./*")
                    .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                    .append(ApplicationConstants.Environment.PWD.$$() + "/*")
                    ;

            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(c.trim());
            }
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");


            if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
                classPathEnv.append(':');
                classPathEnv.append(System.getProperty("java.class.path"));
            }

            appMasterEnv.put("CLASSPATH", classPathEnv.toString());
            System.out.println("Executor CLASSPATH: " + classPathEnv.toString());

            ctx.setEnvironment(appMasterEnv);

            ctx.setCommands(
                    Collections.singletonList( ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java"
                                    + " -Xmx512M"
//                        + " " + myJar
                                    + " com.yyb.learn.Executor"
                                    + " --debug "
                                    + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                                    + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                    )
            );

            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {
        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
            System.out.println("Callback container id : " + containerId.toString());

            if (containers.size() == 1) {
                domainController = container.getNodeId().getHost();
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
            Container container = containers.get(containerId);
            if (container != null) {
                nmClientAsync.getContainerStatusAsync(containerId,
                        container.getNodeId());
            }
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            System.out.println("Container Status: id=" + containerId + ", status="
                    + containerStatus);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            containers.remove(containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable throwable) {
            containers.remove(containerId);
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable throwable) {
            containers.remove(containerId);
        }

        public int getContainerCount() {
            return containers.size();
        }
    }

}
