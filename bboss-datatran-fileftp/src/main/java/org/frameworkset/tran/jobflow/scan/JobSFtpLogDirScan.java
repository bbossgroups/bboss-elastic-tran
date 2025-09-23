package org.frameworkset.tran.jobflow.scan;

import com.frameworkset.util.SimpleStringUtil;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpContext;
import org.frameworkset.tran.ftp.FtpContextImpl;
import org.frameworkset.tran.ftp.SFTPTransfer;
import org.frameworkset.tran.jobflow.DownloadfileConfig;
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
public class JobSFtpLogDirScan extends JobFtpLogDirScan {
    private Logger logger = LoggerFactory.getLogger(JobSFtpLogDirScan.class);

 

    public JobSFtpLogDirScan(DownloadfileConfig downloadfileConfig, FileDownloadService fileDownloadService) {
        super(  downloadfileConfig,  fileDownloadService);
    }
    /**
     * 工作流作业节点调用：识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext){
        FtpContext ftpContext = new FtpContextImpl(ftpConfig,jobFlowNodeExecuteContext,downloadfileConfig.getFileFilter());
        if(logger.isDebugEnabled()){
            if(downloadfileConfig.getFileFilter() == null)
                logger.debug("Scan new sftp file in remote dir {} with filename regex {}.",ftpContext.getRemoteFileDir(),downloadfileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new sftp file in remote dir {} with filename filter {}.",ftpContext.getRemoteFileDir(),
                        downloadfileConfig.getFileFilter().getClass().getCanonicalName());
            }
        }
        List<RemoteResourceInfo> files = SFTPTransfer.ls(ftpContext);

        if(files == null || files.size() == 0){
            if(logger.isInfoEnabled()) {
                logger.warn("Remote ftp dir[{}] is a file or empty directory.", ftpContext.getRemoteFileDir());
            }
            return;
        }
        List<Future> downloadFutures = ftpContext.getFtpConfig().getRemoteFileChannel() != null?new ArrayList<>():null;
        try {
            _scanCheck(jobFlowNodeExecuteContext,"", files, downloadFutures);
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
      


    public void scanSubDirNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,String relativeParentDir,RemoteResourceInfo logDir,List<Future> downloadFutures){
        if(logger.isDebugEnabled()){
            if(downloadfileConfig.getFileFilter() == null)
                logger.debug("Scan new sftp file in remote dir {} with filename regex {}.",logDir.getPath(),downloadfileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new sftp file in remote dir {} with filename filter {}.",logDir.getPath(),
                        downloadfileConfig.getFileFilter().getClass().getCanonicalName());
            }
        }
        FtpContext ftpContext = new FtpContextImpl(ftpConfig,jobFlowNodeExecuteContext,downloadfileConfig.getFileFilter());
        List<RemoteResourceInfo> files = SFTPTransfer.ls(logDir,ftpContext);
        if(files == null || files.size() == 0){
            if(logger.isInfoEnabled()) {
                logger.info("{} must be a directory or is empty directory.", logDir.getPath());
            }
            return;
        }
        _scanCheck(  jobFlowNodeExecuteContext,relativeParentDir,files, downloadFutures);
    }

    /**
     *
     * @param relativeParentDir 相对路径
     * @param files
     */
    private void _scanCheck(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,String relativeParentDir,List<RemoteResourceInfo> files,List<Future> downloadFutures){
        FtpContext ftpContext = new FtpContextImpl(ftpConfig,jobFlowNodeExecuteContext,downloadfileConfig.getFileFilter());
        for(RemoteResourceInfo remoteResourceInfo:files){
            if (jobFlowNodeExecuteContext.assertStopped().isTrue()) {
                break;
            }
            if(remoteResourceInfo.isDirectory()) {
                if(!downloadfileConfig.isScanChild()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Ignore sftp dir:{}", remoteResourceInfo.getPath());
                    }
                }
                else{
                    scanSubDirNewFile(  jobFlowNodeExecuteContext,SimpleStringUtil.getPath(relativeParentDir,remoteResourceInfo.getName()),remoteResourceInfo, downloadFutures);
                }
            }
            else{
                fileDownloadService.checkSFtpNewFile(  jobFlowNodeExecuteContext,relativeParentDir,remoteResourceInfo,ftpContext,downloadFutures);
            }
        }
    }

}
