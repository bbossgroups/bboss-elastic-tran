package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import org.apache.commons.net.ftp.FTPFile;
import org.frameworkset.tran.ftp.FtpContext;
import org.frameworkset.tran.ftp.FtpContextImpl;
import org.frameworkset.tran.ftp.FtpTransfer;
import org.frameworkset.tran.input.RemoteContext;
import org.frameworkset.tran.schedule.TaskContext;
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
public class FtpLogDirScan extends LogDirScan {
    private Logger logger = LoggerFactory.getLogger(FtpLogDirScan.class);
    protected FtpContext ftpContext ;



    public FtpLogDirScan(LogDirsScanThread logDirsScanThread, FileConfig fileConfig, FileListenerService fileListenerService) {
        super( logDirsScanThread,fileConfig,fileListenerService);
        ftpContext = new FtpContextImpl(fileConfig.getFtpConfig());
    }



    /**
     * 识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(TaskContext taskContext){
        if(logger.isDebugEnabled()){
            if(fileConfig.getFileFilter() == null)
                logger.debug("Scan new ftp file in remote dir {} with filename regex {}.",ftpContext.getRemoteFileDir(),fileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new ftp file in remote dir {} with filename filter {}.",ftpContext.getRemoteFileDir(),
                        fileConfig.getFileFilter().getClass().getCanonicalName());
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
                if (!logDirsScanThread.isRunning()) {
                    break;
                }
                if (remoteResourceInfo.isDirectory()) {
                    String name = remoteResourceInfo.getName().trim();
                    if (!fileConfig.isScanChild()) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Ignore ftp dir:{}", name);
                        }
                        continue;
                    } else {
                        scanSubDirNewFile(  taskContext,remoteResourceInfo, name,
                                SimpleStringUtil.getPath(ftpContext.getRemoteFileDir(), name), downloadFutures);
                    }
                } else {
                    fileListenerService.checkFtpNewFile(  taskContext,"", ftpContext.getRemoteFileDir(), remoteResourceInfo, (RemoteContext) ftpContext, downloadFutures);
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

    public void scanSubDirNewFile(TaskContext taskContext,FTPFile logDir,String relativeParentDir,String subdir, List<Future> downloadFutures){
        if(logger.isDebugEnabled()){
            if(fileConfig.getFileFilter() == null)
                logger.debug("Scan new ftp file in remote dir {} with filename regex {}.",subdir,fileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new ftp file in remote dir {} with filename filter {}.",subdir,
                        fileConfig.getFileFilter().getClass().getCanonicalName());
            }
        }
        List<FTPFile> files = FtpTransfer.ls(subdir,ftpContext);
        if(files == null || files.size() == 0){
            if(logger.isDebugEnabled()) {
                logger.debug("{} must be a directory or is empty directory.", subdir);
            }
            return;
        }
        for(FTPFile remoteResourceInfo:files){
            if (!logDirsScanThread.isRunning()) {
                break;
            }
            if(remoteResourceInfo.isDirectory()) {
                String name = remoteResourceInfo.getName().trim();
                if(!fileConfig.isScanChild()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Ignore ftp dir:{}", name);
                    }
                    continue;
                }
                else{
                    scanSubDirNewFile(  taskContext,remoteResourceInfo,SimpleStringUtil.getPath(relativeParentDir,name),
                            SimpleStringUtil.getPath(subdir,name),  downloadFutures);
                }
            }
            else{
                fileListenerService.checkFtpNewFile(  taskContext,relativeParentDir,subdir,remoteResourceInfo,(RemoteContext) ftpContext ,downloadFutures);
            }
        }
    }




}
