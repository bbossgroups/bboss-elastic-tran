package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
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
public class SFtpLogDirScan extends FtpLogDirScan {
    private Logger logger = LoggerFactory.getLogger(SFtpLogDirScan.class);

    /**
     * Constructs a monitor with the specified interval and set of observers.
     *
     * @param logDirsScanThread
     * @param fileListenerService .
     */

    public SFtpLogDirScan(LogDirsScanThread logDirsScanThread, FileConfig fileConfig, FileListenerService fileListenerService) {
        super(  logDirsScanThread,fileConfig,fileListenerService);
    }



    /**
     * 识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(){
        if(logger.isDebugEnabled()){
            if(fileConfig.getFileFilter() == null)
                logger.debug("Scan new sftp file in remote dir {} with filename regex {}.",ftpContext.getRemoteFileDir(),fileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new sftp file in remote dir {} with filename filter {}.",ftpContext.getRemoteFileDir(),
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
        _scanCheck("",files);

    }

    public void scanSubDirNewFile(String relativeParentDir,RemoteResourceInfo logDir){
        if(logger.isDebugEnabled()){
            if(fileConfig.getFileFilter() == null)
                logger.debug("Scan new sftp file in remote dir {} with filename regex {}.",logDir.getPath(),fileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new sftp file in remote dir {} with filename filter {}.",logDir.getPath(),
                        fileConfig.getFileFilter().getClass().getCanonicalName());
            }
        }
        List<RemoteResourceInfo> files = SFTPTransfer.ls(logDir,ftpContext);
        if(files == null || files.size() == 0){
            if(logger.isInfoEnabled()) {
                logger.info("{} must be a directory or is empty directory.", logDir.getPath());
            }
            return;
        }
        _scanCheck(relativeParentDir,files);
    }

    /**
     *
     * @param relativeParentDir 相对路径
     * @param files
     */
    private void _scanCheck(String relativeParentDir,List<RemoteResourceInfo> files){
        for(RemoteResourceInfo remoteResourceInfo:files){
            if (!logDirsScanThread.isRunning()) {
                break;
            }
            if(remoteResourceInfo.isDirectory()) {
                if(!fileConfig.isScanChild()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Ignore sftp dir:{}", remoteResourceInfo.getPath());
                    }
                    continue;
                }
                else{
                    scanSubDirNewFile(SimpleStringUtil.getPath(relativeParentDir,remoteResourceInfo.getName()),remoteResourceInfo);
                }
            }
            else{
                fileListenerService.checkSFtpNewFile(relativeParentDir,remoteResourceInfo,ftpContext);
            }
        }
    }


}
