package org.frameworkset.tran.jobflow.scan;

import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;

/**
 * 扫描新增日志文件
 * @author biaoping.yin
 * @description
 * @create 2021/3/23
 */
public interface JobLogDirScanInf {




    /**
     * 工作流作业节点调用：识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    void scanNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext);
    


    /**
     * 判断是否是本地扫描还是远程扫描，ftp/sftp扫描时返回true
     * @return
     */
    boolean isRemote() ;

    /**
     * 设置远程扫描和本地扫描
     * @param remote
     */
    void setRemote(boolean remote) ;
}
