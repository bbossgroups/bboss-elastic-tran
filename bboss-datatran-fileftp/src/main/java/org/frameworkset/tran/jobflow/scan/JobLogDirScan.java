package org.frameworkset.tran.jobflow.scan;

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.jobflow.DownloadfileConfig;
import org.frameworkset.tran.jobflow.FileDownloadService;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.lifecycle.JobFilenameFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;

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

        if(!downloadfileConfig.isLifecycle()){
            logger.info("本地环境无需下载文件.");
            return ;
        }
        if(logger.isDebugEnabled()){
            logger.debug("scan new log file in dir {} with filename regex {}.",downloadfileConfig.getSourcePath(),downloadfileConfig.getFileNameRegular());
        }
        File logDir = new File(downloadfileConfig.getSourcePath());
        FilenameFilter filter = new JobFilenameFilter(downloadfileConfig.getJobFileFilter(),jobFlowNodeExecuteContext);
        if(logDir.isDirectory() && logDir.exists()){
            File[] files = logDir.listFiles(filter);
            File file = null;
            for(int i = 0; files != null && i < files.length; i ++){
                if (jobFlowNodeExecuteContext.assertStopped().isTrue()) {
                    break;
                }
                file = files[i];

                if(file.isFile() && file.exists()) {
                    if(downloadfileConfig.getFileLiveTime() != null) {
                        if(logger.isInfoEnabled())
                            logger.info("Delete file:{},fileMaxLiveTime:{}毫秒",file.getAbsoluteFile(),downloadfileConfig.getFileLiveTime());
                    }
                    else{
                        if(logger.isInfoEnabled())
                            logger.info("Delete old remote file:{}", file.getAbsoluteFile());
                    }
                    
                    file.delete();
                }
                else if (downloadfileConfig.isScanChild()){ //如果需要扫描子目录
                    scanSubDirNewFile(file.getName(),file,filter,jobFlowNodeExecuteContext);
                }
            }
        }
        else{
            logger.info("{} must be a directory or must be exists.",downloadfileConfig.getSourcePath() );
        }
    }

    public void scanSubDirNewFile(String relativeParent,File logDir,FilenameFilter filter,JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        if(logger.isDebugEnabled()){
            logger.debug("scan new log file in sub dir {}",logDir.getAbsolutePath());
        }
        if(logDir.isDirectory() && logDir.exists()){
            File[] files = logDir.listFiles(filter);
            File file = null;
            for(int i = 0; files != null && i < files.length; i ++){
                if (jobFlowNodeExecuteContext.assertStopped().isTrue()) {
                    break;
                }
                file = files[i];
                if(file.isFile() && file.exists()) {
                    if(downloadfileConfig.getFileLiveTime() != null) {
                        if(logger.isInfoEnabled())
                            logger.info("Delete file:{},fileMaxLiveTime:{}毫秒",file.getAbsoluteFile(),downloadfileConfig.getFileLiveTime());
                    }
                    else{
                        if(logger.isInfoEnabled())
                            logger.info("Delete old remote file:{}", file.getAbsoluteFile());
                    }
                    file.delete();
                }
                else if (downloadfileConfig.isScanChild()){ //如果需要扫描子目录
                    scanSubDirNewFile(SimpleStringUtil.getPath(relativeParent,file.getName()),file,filter,jobFlowNodeExecuteContext);
                }
            }
        }
        else{
            logger.info("{} must be a directory or must be exists.",logDir.getAbsolutePath());
        }
    }

    public boolean isRemote() {
        return remote;
    }

    public void setRemote(boolean remote) {
        this.remote = remote;
    }
}
