package com.yyb.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Hello world!
 *
 */
public class App 
{
    private static Configuration conf;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static void main( String[] args ) throws IOException, YarnException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "center");
        App app = new App();
        conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        app.demo();
    }

    public void demo() throws IOException, YarnException, InterruptedException {
        //yarnClient 负责与集群通信 提交JOb到集群
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        //create an application
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        //set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("appId:" + appId);

        int nodemanagers = yarnClient.getYarnClusterMetrics().getNumNodeManagers();
        System.out.println("nodemanagers:" + nodemanagers);

        Resource resource = appResponse.getMaximumResourceCapability();
        int memory = resource.getMemory();
        int virtualCores = resource.getVirtualCores();
        System.out.println("memory:" + memory);
        System.out.println("virtualCores:" + virtualCores);


        //staging
        String path = "hdfs://nameservice1/user/center/script/jars/";
        String myJar = "a.jar";
        String jarFile = path + myJar;
        Path jarPath = new Path(jarFile);

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);


        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        FileStatus jarFileStatus = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarFileStatus.getLen());
        jarFileStatus.getLen();
        appMasterJar.setTimestamp(jarFileStatus.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
        //注意 jar 文件 在  setLocalResources 的 map 中的 key 一定要以 .jar 结束
        //设置资源，在 APPMaster 启动的时候，Yarn 会帮我们做好资源的处理，自动下载到 Container 中，
        //所以上面的资源一般都要上传到 hdfs 中
        amContainer.setLocalResources(Collections.singletonMap("a.jar", appMasterJar));



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
        System.out.println("CLASSPATH: " + classPathEnv.toString());
        amContainer.setEnvironment(appMasterEnv);

        amContainer.setCommands(
                Collections.singletonList( ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java"
                                + " -Xmx512M"
//                        + " " + myJar
                                + " com.yyb.learn.AppMaster"
                                + " --debug "
                                + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
                                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                )
        );

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(2560);
        capability.setVirtualCores(2);

        appContext.setApplicationName("statistic app");
        appContext.setQueue("default");
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);
        appContext.setMaxAppAttempts(1);

        yarnClient.submitApplication(appContext);


        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED
                && appState != YarnApplicationState.KILLED
                && appState != YarnApplicationState.FAILED) {
            Thread.sleep(1000);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
            System.out.println(sdf.format(new Date()) + " appState: " + appState.toString());
        }
    }
}
