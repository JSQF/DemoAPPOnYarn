package com.yyb.learn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.List;

/**
 * @Author yyb
 * @Description
 * @Date Create in 2019-11-28
 * @Time 17:52
 */
public class AppMaster implements AMRMClientAsync.CallbackHandler {
    private YarnConfiguration conf = new YarnConfiguration();
    private NMClient nmClient;
    private int containerCount = 1;
    public static void main(String[] args) {
        System.out.println("AppMaster: Initializing");
        try {
            new AppMaster().run();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public void run() throws Exception {
        System.out.println("runn");
        Thread.sleep(10000);
        System.out.println("runn");
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
