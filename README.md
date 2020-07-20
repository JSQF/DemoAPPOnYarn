# RunAPPOnYarn  
[gitee地址](#https://gitee.com/jsqf/DemoAPPOnYarn.git)  
[github地址](#https://github.com/JSQF/DemoAPPOnYarn.git)  
本项目的是 写一个可以运行在 Hadoop Yarn 的 APP  ;
须知：在 编写 APPMaster 程序的时候 参考了 Hadoop YARN权威指南 的书籍和代码；[参考代码地址](#https://github.com/jmarkham/yarn-book.git)  
# Start
### APP  
APP 是 主运行程序  ，如果网络条件和配置文件（需要配置文件放在 resources 目录下）允许的情况下可以在本地调试。  
APP 程序主要是 获取到 yarnClient ，通过 yarnClient 再获取到 NewApplicationResponse 和 ApplicationSubmissionContext 和 YarnClusterMetrics  
YarnClusterMetrics 里面会有 Yarn 集群的 NumNodeManagers等信息；  
NewApplicationResponse 里面可以获取到 Yarn 集群的 一些资源信息，比如 memory和virtualCores；  
ApplicationSubmissionContext 则可以 设置 ApplicationName、Queue、Resource（Memory And VirtualCores）、AMContainerSpec、MaxAppAttempts；  
AMContainerSpec 里面是 需要设置 启动 APPMaster 的 运行资源：包含 classpath，启动命令和LocalResources；
最后 yarnClient 提交 这个 ApplicationSubmissionContext 对象。  

### APPMaster  
#### RMCallbackHandler  
AM ResourceManager 异步 回调 Handler   
#### NMCallbackHandler  
NodeManager 异步 回调 Handler  
#### LaunchContainerRunnable  
专门的 Container 启动 线程   