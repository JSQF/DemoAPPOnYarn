package com.yyb.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2019-11-28
 * @Time 17:52
 */
public class AppMaster implements AMRMClientAsync.CallbackHandler {
    private static YarnConfiguration conf = new YarnConfiguration();
    private NMClient nmClient;
    private int containerCount = 1;
    public static void main(String[] args) {
        System.out.println("AppMaster: Initializing");
        try {
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            Map<String, String> envs = System.getenv();

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

            AMRMClient<AMRMClient.ContainerRequest> amClient = AMRMClient.createAMRMClient();
            amClient.init(conf);
            amClient.start();
            InetAddress address = InetAddress.getLocalHost();
            amClient.registerApplicationMaster(address.getHostAddress(), 0, "");

            new AppMaster().run();
            System.out.println("finished !");
            System.exit(0);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public void run() throws Exception {
        for(int i=0; i<100; i++){
            System.out.println("running ... ");
            Thread.sleep(1000);
        }
    }
    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {

    }

    @Override
    public void onContainersAllocated(List<Container> list) {

    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {

    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable throwable) {

    }
}
