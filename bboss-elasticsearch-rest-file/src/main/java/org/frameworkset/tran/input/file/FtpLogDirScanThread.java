package org.frameworkset.tran.input.file;

import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpContext;
import org.frameworkset.tran.ftp.FtpContextImpl;
import org.frameworkset.tran.ftp.SFTPTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 扫描ftp服务器上面指定目录下面新增日志文件
 * @author biaoping.yin
 * @description
 * @create 2021/3/23
 */
public class FtpLogDirScanThread extends LogDirScanThread{
    private Logger logger = LoggerFactory.getLogger(FtpLogDirScanThread.class);
    private FtpContext ftpContext ;

    /**
     * Constructs a monitor with the specified interval and set of observers.
     *
     * @param interval The amount of time in milliseconds to wait between
     * checks of the file system
     * @param fileListenerService .
     */
    public FtpLogDirScanThread(final long interval, FtpConfig fileConfig, FileListenerService fileListenerService) {
       super(interval,fileConfig,fileListenerService);
       ftpContext = new FtpContextImpl(fileConfig);
    }



    /**
     * 识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(){
        if(logger.isDebugEnabled()){
            if(fileConfig.getFileFilter() == null)
                logger.debug("Scan new ftp file in remote dir {} with filename regex {}.",ftpContext.getRemoteFileDir(),fileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new ftp file in remote dir {} with filename filter {}.",ftpContext.getRemoteFileDir(),
                        fileConfig.getFileFilter().getClass().getCanonicalName());
            }
        }
        List<RemoteResourceInfo> files = SFTPTransfer.ls(ftpContext);
        if(files == null || files.size() == 0){
            if(logger.isInfoEnabled()) {
                logger.info("{} must be a directory or is empty directory.", ftpContext.getRemoteFileDir());
            }
            return;
        }
        for(RemoteResourceInfo remoteResourceInfo:files){
            if(remoteResourceInfo.isDirectory()) {
                if(logger.isInfoEnabled()) {
                    logger.info("Ignore ftp dir:{}",remoteResourceInfo.getPath());
                }
                continue;
            }
            else{
                fileListenerService.checkFtpNewFile(remoteResourceInfo,ftpContext.getFtpConfig(),ftpContext);
            }
        }

    }




}
