package org.frameworkset.tran.jobflow.scan;

import com.frameworkset.util.SimpleStringUtil;
import org.apache.commons.net.ftp.FTPFile;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpContext;
import org.frameworkset.tran.ftp.FtpContextImpl;
import org.frameworkset.tran.ftp.FtpTransfer;
import org.frameworkset.tran.input.RemoteContext;
import org.frameworkset.tran.jobflow.FileDownloadService;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 扫描ftp服务器上面指定目录下面新增日志文件
 * @author biaoping.yin
 * @description
 * @create 2021/3/23
 */
public class JobFtpLogDirScan extends JobLogDirScan {
    private Logger logger = LoggerFactory.getLogger(JobFtpLogDirScan.class);

    protected FtpConfig ftpConfig;

 

    public JobFtpLogDirScan(FtpConfig ftpConfig, FileDownloadService fileDownloadService) {
        super(   fileDownloadService);
        this.ftpConfig = ftpConfig;
        
    }

    /**
     * 工作流作业节点调用：识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        FtpContext ftpContext = new FtpContextImpl(ftpConfig,jobFlowNodeExecuteContext,fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().getFileFilter());
        if(logger.isDebugEnabled()){
            if(fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().getFileFilter() == null)
                logger.debug("Scan new ftp file in remote dir {} with filename regex {}.",ftpContext.getRemoteFileDir(),fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().getFileNameRegular());
            else{
                logger.debug("Scan new ftp file in remote dir {} with filename filter {}.",ftpContext.getRemoteFileDir(),
                        fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().getFileFilter().getClass().getCanonicalName());
            }
        }
        List<FTPFile> files = FtpTransfer.ls(ftpContext);
        if(files == null || files.size() == 0){
            if(logger.isDebugEnabled()) {
                logger.debug("{} must be a directory or is empty directory.", ftpContext.getRemoteFileDir());
            }
            return;
        }
        List<Future> downloadFutures = ftpContext.getFtpConfig().getRemoteFileChannel() != null?new ArrayList<>():null;
        try {
            for (FTPFile remoteResourceInfo : files) {
                if (jobFlowNodeExecuteContext.assertStopped().isTrue()) {
                    break;
                }
                if (remoteResourceInfo.isDirectory()) {
                    String name = remoteResourceInfo.getName().trim();
                    if (!fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().isScanChild()) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Ignore ftp dir:{}", name);
                        }
                        continue;
                    } else {
                        scanSubDirNewFile(    jobFlowNodeExecuteContext,remoteResourceInfo, name,
                                SimpleStringUtil.getPath(ftpContext.getRemoteFileDir(), name), downloadFutures);
                    }
                } else {
                    fileDownloadService.checkFtpNewFile(  jobFlowNodeExecuteContext,"", ftpContext.getRemoteFileDir(), remoteResourceInfo, (RemoteContext) ftpContext, downloadFutures);
                }
            }
        }
        finally {
            if(downloadFutures != null && downloadFutures.size() > 0){
                for(Future future:downloadFutures){
                    try {
                        future.get();
                    } catch (InterruptedException e) {

                    } catch (ExecutionException e) {

                    }
                }
            }
        }
    }
  


    public void scanSubDirNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,FTPFile logDir,String relativeParentDir,String subdir, List<Future> downloadFutures){
        if(logger.isDebugEnabled()){
            if(fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().getFileFilter() == null)
                logger.debug("Scan new ftp file in remote dir {} with filename regex {}.",subdir,fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().getFileNameRegular());
            else{
                logger.debug("Scan new ftp file in remote dir {} with filename filter {}.",subdir,
                        fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().getFileFilter().getClass().getCanonicalName());
            }
        }
        FtpContext ftpContext = new FtpContextImpl(ftpConfig,jobFlowNodeExecuteContext,fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().getFileFilter());
        List<FTPFile> files = FtpTransfer.ls(subdir,ftpContext);
        if(files == null || files.size() == 0){
            if(logger.isDebugEnabled()) {
                logger.debug("{} must be a directory or is empty directory.", subdir);
            }
            return;
        }
        for(FTPFile remoteResourceInfo:files){
            if (jobFlowNodeExecuteContext.assertStopped().isTrue()) {
                break;
            }
            if(remoteResourceInfo.isDirectory()) {
                String name = remoteResourceInfo.getName().trim();
                if(!fileDownloadService.getRemoteFileInputJobFlowNodeBuilder().isScanChild()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Ignore ftp dir:{}", name);
                    }                   
                }
                else{
                    scanSubDirNewFile(  jobFlowNodeExecuteContext,remoteResourceInfo,SimpleStringUtil.getPath(relativeParentDir,name),
                            SimpleStringUtil.getPath(subdir,name),  downloadFutures);
                }
            }
            else{
                fileDownloadService.checkFtpNewFile(  jobFlowNodeExecuteContext,relativeParentDir,subdir,remoteResourceInfo,(RemoteContext) ftpContext ,downloadFutures);
            }
        }
    }




}
