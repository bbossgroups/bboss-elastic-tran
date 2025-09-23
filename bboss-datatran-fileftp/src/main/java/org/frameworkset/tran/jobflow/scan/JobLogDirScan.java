package org.frameworkset.tran.jobflow.scan;

import org.frameworkset.tran.jobflow.DownloadfileConfig;
import org.frameworkset.tran.jobflow.FileDownloadService;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 扫描新增日志文件
 * @author biaoping.yin
 * @description
 * @create 2021/3/23
 */
public class JobLogDirScan implements JobLogDirScanInf {
    private Logger logger = LoggerFactory.getLogger(JobLogDirScan.class);
    protected FileDownloadService fileDownloadService;
    private boolean remote;

    protected DownloadfileConfig downloadfileConfig;
    


    
    
    public JobLogDirScan(DownloadfileConfig downloadfileConfig,FileDownloadService fileDownloadService){
        this.fileDownloadService = fileDownloadService;
        this.downloadfileConfig = downloadfileConfig;
    }


 


    /**
     * 工作流作业节点调用：识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        
    }
     
 

    public boolean isRemote() {
        return remote;
    }

    public void setRemote(boolean remote) {
        this.remote = remote;
    }
}
