package org.frameworkset.tran.input.s3;

import com.frameworkset.util.SimpleStringUtil;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.frameworkset.nosql.s3.OSSClient;
import org.frameworkset.nosql.s3.OSSFile;
import org.frameworkset.nosql.s3.OSSStartResult;
import org.frameworkset.tran.ftp.SFTPTransfer;
import org.frameworkset.tran.input.file.*;
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
public class S3DirScan extends LogDirScan {
    private Logger logger = LoggerFactory.getLogger(S3DirScan.class);
    private OSSClient ossClient ;
    private OSSFileInputConfig ossFileConfig;
    /**
     * Constructs a monitor with the specified interval and set of observers.
     *
     * @param logDirsScanThread
     * @param fileListenerService .
     */

    public S3DirScan(LogDirsScanThread logDirsScanThread, FileConfig fileConfig, FileListenerService fileListenerService) {
        super(  logDirsScanThread,fileConfig,fileListenerService);
        this.ossFileConfig = fileConfig.getOssFileInputConfig();
        this.ossClient = ossFileConfig.getOssClient();
    }



    /**
     * 识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    @Override
    public void scanNewFile(TaskContext taskContext){
        if(logger.isDebugEnabled()){
            if(fileConfig.getFileFilter() == null)
                logger.debug("Scan new oss file in remote dir {} with filename regex {}.",ossFileConfig.getRemoteFileDir(),fileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new oss file in remote dir {} with filename filter {}.",ossFileConfig.getRemoteFileDir(),
                        fileConfig.getFileFilter().getClass().getCanonicalName());
            }
        }
        FileFilter fileFilter = fileConfig.getFileFilter();
        List<OSSFile> files = ossClient.listOssFile(ossFileConfig.getBucket(),ossFileConfig.getRemoteFileDir(), fileConfig.isScanChild());

        if(fileFilter != null){
            List<OSSFile> newFiles = new ArrayList<>(); 
            for(int i = 0; i < files.size(); i ++ ){
                OSSFile file = files.get(i);
                FilterFileInfo fileInfo = new OSSFilterFileInfo(file);
                if(fileFilter.accept(fileInfo,fileConfig)){
                    newFiles.add(file);
                }
            }
            files = newFiles;
        }
        if(files == null || files.size() == 0){
            if(logger.isInfoEnabled()) {
                logger.warn("Remote oss dir[{}] is a file or empty directory.", ossFileConfig.getRemoteFileDir());
            }
            return;
        }
        List<Future> downloadFutures = ossFileConfig.getRemoteFileChannel() != null?new ArrayList<>():null;
        try {
            _scanCheck(taskContext,"", files, downloadFutures);
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

    public void scanSubDirNewFile(TaskContext taskContext,String relativeParentDir,OSSFile logDir,List<Future> downloadFutures){
        String path = SimpleStringUtil.getPath(relativeParentDir,logDir.getObjectName());
        if(logger.isDebugEnabled()){
            if(fileConfig.getFileFilter() == null)
                logger.debug("Scan new oss file in remote dir {} with filename regex {}.",path,fileConfig.getFileNameRegular());
            else{
                logger.debug("Scan new oss file in remote dir {} with filename filter {}.",path,
                        fileConfig.getFileFilter().getClass().getCanonicalName());
            }
        }
        
        List<OSSFile> files = ossClient.listOssFile(ossFileConfig.getBucket(),null);
        if(files == null || files.size() == 0){
            if(logger.isInfoEnabled()) {
                logger.info("{} must be a directory or is empty directory.",path);
            }
            return;
        }
        _scanCheck(  taskContext,relativeParentDir,files, downloadFutures);
    }

    /**
     *
     * @param relativeParentDir 相对路径
     * @param files
     */
    private void _scanCheck(TaskContext taskContext,String relativeParentDir,List<OSSFile> files,List<Future> downloadFutures){
        for(OSSFile remoteResourceInfo:files){
            if (!logDirsScanThread.isRunning()) {
                break;
            }
            if(remoteResourceInfo.isDir()) {
                String path = SimpleStringUtil.getPath(relativeParentDir,remoteResourceInfo.getObjectName());
                if(!fileConfig.isScanChild()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Ignore oss dir:{}", path);
                    }
                    continue;
                }
                else{
                    scanSubDirNewFile(  taskContext,path,remoteResourceInfo, downloadFutures);
                }
            }
            else{
                fileListenerService.checkOSSNewFile(  taskContext,relativeParentDir,remoteResourceInfo,this.ossFileConfig,downloadFutures,ossClient);
            }
        }
    }


}
