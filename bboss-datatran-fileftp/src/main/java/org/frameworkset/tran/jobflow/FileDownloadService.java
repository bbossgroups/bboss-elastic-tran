package org.frameworkset.tran.jobflow;

import com.frameworkset.util.SimpleStringUtil;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.apache.commons.net.ftp.FTPFile;
import org.frameworkset.nosql.s3.OSSClient;
import org.frameworkset.nosql.s3.OSSFile;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.ftp.*;
import org.frameworkset.tran.input.RemoteContext;
import org.frameworkset.tran.input.file.DownAction;
import org.frameworkset.tran.input.file.FileCheckResult;
import org.frameworkset.tran.input.file.RemoteFileChannel;
import org.frameworkset.tran.input.s3.OSSFileInputConfig;
import org.frameworkset.tran.input.zipfile.ArchiveExtractor;
import org.frameworkset.tran.input.zipfile.Zip4jExtractor;
import org.frameworkset.tran.input.zipfile.ZipFilePasswordFunction;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class FileDownloadService {
    private static Logger logger = LoggerFactory.getLogger(FileDownloadService.class);
    private Map<String,Object> downLoadTraces ;
 
    private Object dummy = new Object();
    private DownloadJobFlowNodeFunction downloadJobFlowNodeFunction;
    private Lock lock = new ReentrantLock();
    public FileDownloadService(DownloadJobFlowNodeFunction downloadJobFlowNodeFunction) {
        downLoadTraces = new HashMap<>();
        this.downloadJobFlowNodeFunction = downloadJobFlowNodeFunction;
    }



    public DownloadfileConfig getDownloadfileConfig() {
        return this.downloadJobFlowNodeFunction.getDownloadfileConfig();
    }




    public void checkSFtpNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, String relativeParentDir, RemoteResourceInfo remoteResourceInfo, final FtpContext ftpContext, List<Future> downloadFutures) {
        checkRemoteNewFile(  jobFlowNodeExecuteContext, relativeParentDir,remoteResourceInfo.getName(), remoteResourceInfo.getPath(),  ftpContext.getFtpConfig(), new RemoteFileAction() {
            @Override
            public boolean downloadFile(String localFile,String remoteFile) {
                SFTPTransfer.downloadFile(ftpContext,remoteFile,localFile);
                return true;
            }

            @Override
            public void deleteFile(String remoteFile) {
                SFTPTransfer.deleteFile(ftpContext,remoteFile);
            }
        },  downloadFutures);


    }


    public void checkOSSNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, String relativeParentDir,
                                OSSFile remoteResourceInfo, final OSSFileInputConfig ossFileInputConfig, List<Future> downloadFutures, OSSClient ossClient) {
        checkRemoteNewFile(  jobFlowNodeExecuteContext, relativeParentDir,remoteResourceInfo.getObjectName(), remoteResourceInfo.getObjectName(),  ossFileInputConfig, new RemoteFileAction() {
            @Override
            public boolean downloadFile(String localFile,String remoteFile) {
                ossClient.downloadObject(ossFileInputConfig.getBucket(),remoteFile,localFile);
                return true;
            }

            @Override
            public void deleteFile(String remoteFile) {
                ossClient.deleteOssFile(ossFileInputConfig.getBucket(),remoteFile);
            }
        },  downloadFutures);


    }


    private synchronized void checkParentExist(File handleFile){
        File parent = handleFile.getParentFile();
        try {

            if (!parent.exists()) {
                parent.mkdirs();
            }
        }
        catch (Exception e){
            logger.warn("Create parent dir " + parent.getAbsolutePath() + " failed:");
        }
    }


    private RemoteFileValidate.Result remoteFileValidate(String remoteFile, File dataFile, RemoteContext ftpContext, RemoteFileAction remoteFileAction, DownAction downAction, boolean redown){
        RemoteFileValidate remoteFileValidate = ftpContext.getRemoteFileValidate();
        if(remoteFileValidate == null)
            return RemoteFileValidate.Result.default_ok;
        RemoteFileValidate.Result result = remoteFileValidate.validateFile(new ValidateContext(dataFile,remoteFile,ftpContext,remoteFileAction,redown));

        if( result.getValidateResult() == RemoteFileValidate.FILE_VALIDATE_FAILED_DELETE){
           if(!redown) {
               dataFile.delete();
               if (result.getMessage() != null ) {

                   logger.warn("FILE_VALIDATE_FAILED file {},remotePath:{}  failed:{}", dataFile.getAbsolutePath(), remoteFile,result.getMessage());
               }
           }
        }
        else   if( result.getValidateResult() == RemoteFileValidate.FILE_VALIDATE_FAILED){
            if(!redown) {

                if (result.getMessage() != null ) {

                    logger.warn("FILE_VALIDATE_FAILED file {},remotePath:{}  failed:{}", dataFile.getAbsolutePath(), remoteFile,result.getMessage());
                }
            }
        }
        else if(result.getValidateResult() == RemoteFileValidate.FILE_VALIDATE_FAILED_REDOWNLOAD ){
            if(!redown) {
                if (result.getMessage() != null ) {

                    logger.warn("FILE_VALIDATE_FAILED file {},remotePath:{}  failed:{}", dataFile.getAbsolutePath(), remoteFile,result.getMessage());
                }
                int downloadCounts = result.getRedownloadCounts();
                if (downloadCounts > 0) {
                    int tmp = 0;
                    do {
                        try {
                            dataFile.delete();//删除文件，再重新下载
                        } catch (Exception e) {
                            logger.warn("FILE_VALIDATE_FAILED_REDOWNLOAD delete file " + dataFile.getAbsolutePath() + ",remotePath:"+remoteFile+" failed:", e);
                        }
                        try {
                            Thread.currentThread().sleep(10000l);//等待10秒后下载
                        } catch (InterruptedException e) {
                            logger.warn("", e);
                        }

                        downAction.down(tmp,true);//重下文件
                        if(!dataFile.exists()){
                            continue;
                        }
                        result = remoteFileValidate(remoteFile,dataFile, ftpContext, remoteFileAction, downAction, true);
                        if (result.isOk()) {
                            logger.info("File {} ,RemotePath:{} 重试{}次后下载成功.", dataFile.getAbsolutePath(),remoteFile,tmp + 1);
                            break;
                        }

                        tmp++;
                        if (result.getMessage() != null ) {

                            logger.warn("FILE_VALIDATE_FAILED file {} ,remotePath:{} failed:{}", dataFile.getAbsolutePath(),remoteFile,result.getMessage());
                        }
                        if (tmp == downloadCounts) {
                            break;
                        }
                    } while (true);
                }
                try {
                    if (!result.isOk() && dataFile.exists())//
                        dataFile.delete();//删除文件，再重新下载
                } catch (Exception e) {
                    logger.warn("FILE_VALIDATE_FAILED_REDOWNLOAD delete file " + dataFile.getAbsolutePath() + ",remotePath:"+remoteFile+" failed:", e);
                }
            }
        }

        return result;


    }

    private FileCheckResult checkFileIdIsNew(String fileId){
        FileCheckResult fileCheckResult = new FileCheckResult();
 
    
        fileCheckResult.setResult(FileCheckResult.FileCheckResult_NewFile);

        return fileCheckResult;
    }
    
    
    /**
     *
     * @param relativeParentDir
     * @param fileName 远程文件名称
     * @param remoteFile  远程根目录+relativeParentDir
     * @param remoteContext
     * @param remoteFileAction
     */
    private void checkRemoteNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, final String relativeParentDir, String fileName, final String remoteFile,
                                    final RemoteContext remoteContext, final RemoteFileAction remoteFileAction, List<Future> downloadFutures) {
        if(this.getDownloadfileConfig().isLifecycle()){
            RemoteFileChannel remoteFileChannel = remoteContext.getRemoteFileChannel();
            if(remoteFileChannel != null) { //异步并行下载或者清理文件
                Future future = remoteFileChannel.submitNewTask(new Runnable() {
                    @Override
                    public void run() {

                        if(logger.isInfoEnabled()){
                            if(getDownloadfileConfig().getFileLiveTime() != null) {
                                logger.info("Delete old remote file:{},fileMaxLiveTime:{}毫秒", remoteFile, getDownloadfileConfig().getFileLiveTime());
                            }
                            else{
                                logger.info("Delete old remote file:{}", remoteFile);
                            }
                        }
                        remoteFileAction.deleteFile(remoteFile);

                    }
                });
                downloadFutures.add(future);
            }
            else{ //同步串行下载或者清理文件


                if(logger.isInfoEnabled()){
                    if(getDownloadfileConfig().getFileLiveTime() != null) {
                        logger.info("Delete old remote file:{},fileMaxLiveTime:{}毫秒", remoteFile,getDownloadfileConfig().getFileLiveTime());
                    }
                    else{
                        logger.info("Delete old remote file:{}", remoteFile);
                    }
                    
                }
                remoteFileAction.deleteFile(remoteFile);
            }
           
            return;
        }        
        String sourcePath = remoteContext.getSourcePath();
        final File handleFile = new File( SimpleStringUtil.getPath(sourcePath,relativeParentDir), fileName);//正式文件,如果有子目录，则需要保存到子目录
        checkParentExist(handleFile);
        final File localFile = new File(SimpleStringUtil.getPath(remoteContext.getDownloadTempDir(),relativeParentDir),fileName);//临时下载文件，,如果有子目录，则需要保存到临时子目录，下载完毕后重命名为正式文件，如果正式文件不存在，需重新下载文件
        checkParentExist(localFile);
        final String fileId = FileInodeHandler.change(handleFile.getAbsolutePath());//ftp下载的文件直接使用文件路径作为fileId
        boolean isNewFile = false;
        boolean isDowning = false;
        FileCheckResult fileCheckResult = null;
        lock.lock();
        try {

            if(jobFlowNodeExecuteContext.assertStopped().isTrue() )
                return;
            fileCheckResult = checkFileIdIsNew(fileId);
            isNewFile = fileCheckResult.getResult() == FileCheckResult.FileCheckResult_NewFile;

            if(isNewFile ) {
                if (!downLoadTraces.containsKey(fileId)) {
                    downLoadTraces.put(fileId, dummy);
                } else {
                    isDowning = true;
                    if(logger.isDebugEnabled()){
                        logger.debug("JobFlow FILE_DOWNLOAD_TRACE file {} ,remotePath:{} is downloading or downloaded.", handleFile.getAbsolutePath(),remoteFile);
                    }
                }
            }
            else if(handleFile.isFile() && handleFile.exists()){
//                this.getFileInputDataTranPlugin().handleCompleteFiles(fileCheckResult.getResult(),handleFile);
            }


        } finally {

            lock.unlock();
        }
        if(isNewFile && !isDowning) {
			RemoteFileChannel remoteFileChannel = remoteContext.getRemoteFileChannel();
        	if(remoteFileChannel != null) { //异步并行下载或者清理文件
				Future future = remoteFileChannel.submitNewTask(new Runnable() {
					@Override
					public void run() {

                        try {
                            downAndCollectFile(jobFlowNodeExecuteContext, handleFile, localFile, fileId,
                                    relativeParentDir, remoteFile, remoteContext, remoteFileAction);
                        }
                        catch (Exception e){

                            if(logger.isErrorEnabled())
                                logger.error("Download remoteFile" + remoteFile+ " to " + localFile+ " And Collect File failed：",e);
                        }
                       
					}
				});
                downloadFutures.add(future);
			}
        	else{ //同步串行下载或者清理文件


                try {
                    downAndCollectFile(  jobFlowNodeExecuteContext,handleFile, localFile, fileId,
                            relativeParentDir, remoteFile, remoteContext, remoteFileAction);
                }
                catch (Exception e){

                    if(logger.isErrorEnabled())
                        logger.error("Download remoteFile" + remoteFile+ " to " + localFile+ " And Collect File failed：",e);
                }
			}
        }

    }

    /**
     * 文件没有下载成功，去掉下载跟踪记录
     * @param fileId
     */
    private void removeDownTrace(String fileId){
        lock.lock();
        try{

            downLoadTraces.remove(fileId);
        }
        finally {
            lock.unlock();
        }
    }


    private void downAndCollectFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext, File handleFile, File localFile, String fileId ,
                                    String relativeParentDir, final String remoteFile, RemoteContext ftpContext, final RemoteFileAction remoteFileAction){
        /**
         * 如果处理文件不存在，则下载文件到本地临时目录,下载后重命名为正式处理文件，避免因下载中断处理不完整文件问题
         */
        String localFilePath = localFile.getAbsolutePath();
        DownloadFileMetrics downloadFileMetrics = new DownloadFileMetrics();
        downloadFileMetrics.setLocalFilePath(handleFile.getAbsolutePath());
        downloadFileMetrics.setRemoteFilePath(remoteFile);
        boolean exits = handleFile.exists();
        if(exits){
            if(ftpContext.isReplaceExistFile()){
                if(logger.isInfoEnabled())
                    logger.info("ReplaceExistFile is true and delete old file and redown file:{}", handleFile.getAbsoluteFile());
                boolean deleted = handleFile.delete();
                if(!deleted){
                    if(logger.isWarnEnabled())
                        logger.warn("Delete old file:{} failed", handleFile.getAbsoluteFile());
                }
            }
        }
        if(!handleFile.exists()) {
            DownloadedFileRecorder downloadedFileRecorder = downloadJobFlowNodeFunction.getDownloadedFileRecord();
            
           
            try {
                if(!downloadedFileRecorder.recordBeforeDownload(downloadFileMetrics,jobFlowNodeExecuteContext)) {
                    if(logger.isDebugEnabled()){
                        logger.debug("Ignore already downloaded file:{}", handleFile.getAbsoluteFile());
                    }
                    return;
                }
                /**
                 * 支持断点续传
                 */
//                    SFTPTransfer.downloadFile(ftpContext,remoteFile,fileConfig.getDownloadTempDir());
                DownAction downAction = new DownAction() {
                    @Override
                    public void down(int times, boolean redown) {
                        String msg = null;
                        StringBuilder builder = new StringBuilder();
                        if (redown) {
                           
                            builder.append("开始第").append(times + 1).append("次重试下载文件：localPath:").append(localFilePath).append(",remotePath:").append(remoteFile).append("");
                            msg = builder.toString();
                            if(logger.isWarnEnabled())
                                logger.warn(msg);
                            
                        }
                        else{
                            
                            builder.append("开始下载文件：localPath:").append(localFilePath).append(",remotePath:").append(remoteFile).append("");
                            msg = builder.toString();
                            if(logger.isInfoEnabled())
                                logger.info(msg);
                        }
                        builder.setLength(0);
//                        dataTranPlugin.reportJobMetricLog(taskContext,msg);
                         
                         
                        long start = System.currentTimeMillis();
                       
                      
                        remoteFileAction.downloadFile(localFilePath, remoteFile);
                       
                        long elapsed = System.currentTimeMillis() - start ;
                        downloadFileMetrics.setElapsed(elapsed);
                        if (!localFile.exists()) {
                            
                            if (!redown) {
                                builder.append("下载文件失败：localPath:").append(localFilePath).append(",remotePath:").append(remoteFile)
                                        .append(",耗时:").append(elapsed).append("毫秒!");
                                msg = builder.toString();
                                builder.setLength(0);
                                if(logger.isWarnEnabled())
                                    logger.warn(msg);
                            } else {
                                builder.append("第").append(times + 1).append("次重试下载文件失败：localPath:").append(localFilePath)
                                        .append(",remotePath:").append(remoteFile).append(",耗时:").append(elapsed).append("毫秒!");
                                msg = builder.toString();
                                builder.setLength(0);
                                if(logger.isWarnEnabled())
                                    logger.warn(msg);
                            }

                        }
                        else{
                            builder.append("下载文件完成：localPath:").append(localFilePath).append(",remotePath:").append(remoteFile)
                                    .append(",耗时:").append(elapsed).append("毫秒!");
                            msg = builder.toString();
                            builder.setLength(0);
                            if(logger.isInfoEnabled())
                                logger.info(msg);
                        }
                    }
                };
                downAction.down(-1, false);
                if (!localFile.exists()) {
                    removeDownTrace(fileId);
                    downloadFileMetrics.setMessage("localFile not exists:"+localFilePath);
                    downloadedFileRecorder.recordAfterDownload(downloadFileMetrics,jobFlowNodeExecuteContext,null);
                    return;
                }

                if(ftpContext.getRemoteFileValidate() != null) {
                    long startTime = System.currentTimeMillis();
                    RemoteFileValidate.Result result = remoteFileValidate(remoteFile, localFile, ftpContext, remoteFileAction, downAction, false);
                    long endTime = System.currentTimeMillis();
                    downloadFileMetrics.setValidateElapsed(endTime - startTime);
                    if (!result.isOk()) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("文件校验失败：localPath:")
                                .append(localFilePath).append(",remotePath:")
                                .append(remoteFile).append(",校验耗时：")
                                .append(downloadFileMetrics.getValidateElapsed()).append("毫秒，失败原因：").append(result.getMessage());
                        String msg = builder.toString();
                        if(logger.isWarnEnabled())
                            logger.warn(msg);

//                        dataTranPlugin.reportJobMetricWarn(taskContext,msg);
                        removeDownTrace(fileId);
                        downloadFileMetrics.setMessage(msg);
                        return;
                    }
                    else{
                        if(logger.isInfoEnabled()) {
                            StringBuilder builder = new StringBuilder();
                            builder.append("文件校验成功：localPath:")
                                    .append(localFilePath).append(",remotePath:")
                                    .append(remoteFile).append(",校验耗时：")
                                    .append(downloadFileMetrics.getValidateElapsed()).append("毫秒 ");
                            String msg = builder.toString();
                            logger.info(msg);
                        }
//                        if(taskContext != null) {
//                            taskContext.reportJobMetricLog(msg);
//                        }
                    }
                }
                boolean renamesuccess = localFile.renameTo(handleFile);
                if (renamesuccess) {
                    if (logger.isInfoEnabled())
                        logger.info("Rename " + localFilePath + " to " + downloadFileMetrics.getLocalFilePath());
                    //开始解压zip文件：
                    if(ftpContext.isUnzip()){
                        if(logger.isInfoEnabled())
                            logger.info("Start unzip file:"+downloadFileMetrics.getLocalFilePath());
                        long startTime = System.currentTimeMillis();
                        ZipFilePasswordFunction zipFilePasswordFunction = ftpContext.getZipFilePasswordFunction();
                        String zipFilePassward = zipFilePasswordFunction == null?ftpContext.getZipFilePassward():
                                            zipFilePasswordFunction.getZipFilePassword(jobFlowNodeExecuteContext,downloadFileMetrics.getRemoteFilePath(),downloadFileMetrics.getLocalFilePath());
                        
                        int files = Zip4jExtractor.extractEncryptedZip(handleFile,ftpContext.getUnzipDir(),zipFilePassward);
                        long endTime = System.currentTimeMillis();
                        downloadFileMetrics.setUnzipElapsed(endTime - startTime);
                        downloadFileMetrics.setFiles(files);
                        if(ftpContext.isDeleteZipFileAfterUnzip()){
                            handleFile.delete();
                            if(logger.isInfoEnabled())
                                logger.info("Delete zip file:"+downloadFileMetrics.getLocalFilePath());
                        }
                    }
                    //开始解压tar文件：
                    else if(ftpContext.isUntar()){
                        if(logger.isInfoEnabled())
                            logger.info("Start untar file:"+downloadFileMetrics.getLocalFilePath());
                        long startTime = System.currentTimeMillis();

                        int files = ArchiveExtractor.extract(handleFile,ftpContext.getUnzipDir());
                        long endTime = System.currentTimeMillis();
                        downloadFileMetrics.setUnzipElapsed(endTime - startTime);
                        downloadFileMetrics.setFiles(files);
                        if(ftpContext.isDeleteZipFileAfterUnzip()){
                            handleFile.delete();
                            if(logger.isInfoEnabled())
                                logger.info("Delete tar file:"+downloadFileMetrics.getLocalFilePath());
                        }
                    }
//                    removeDownTrace(fileId);
                    downloadedFileRecorder.recordAfterDownload(downloadFileMetrics, jobFlowNodeExecuteContext,null);
                    
                } else {
                    removeDownTrace(fileId);
                    StringBuilder builder = new StringBuilder();
                    builder.append("文件下载后重命名失败：tempPath:")
                                .append(localFilePath).append(",remotePath:")
                                .append(remoteFile).append(",handle file path:")
                                .append(downloadFileMetrics.getLocalFilePath());
                    String msg = builder.toString();

//                    dataTranPlugin.reportJobMetricWarn(taskContext,msg);
                    if(logger.isWarnEnabled())
                        logger.warn(msg);
                    downloadFileMetrics.setMessage(msg);
                    downloadedFileRecorder.recordAfterDownload(downloadFileMetrics, jobFlowNodeExecuteContext,null);
                    return;
                }
            }
            catch (Exception e){
                removeDownTrace(fileId);
                downloadFileMetrics.setMessage("下载文件"+remoteFile + "到"+downloadFileMetrics.getLocalFilePath()+"失败");
                downloadedFileRecorder.recordAfterDownload(downloadFileMetrics, jobFlowNodeExecuteContext,e);
                throw new FileDownException(downloadFileMetrics.getMessage(),e);
            }
            catch (Throwable e){
                removeDownTrace(fileId);
                downloadFileMetrics.setMessage("下载文件"+remoteFile + "到"+downloadFileMetrics.getLocalFilePath()+"失败");
                downloadedFileRecorder.recordAfterDownload(downloadFileMetrics, jobFlowNodeExecuteContext,e);
                throw new FileDownException(downloadFileMetrics.getMessage(),e);
            }
        }
        else{
            if(logger.isDebugEnabled())
                logger.debug("文件：{}已存在，忽略下载",downloadFileMetrics.getLocalFilePath());
        }

        if(!handleFile.exists()){
            if(logger.isWarnEnabled())
                logger.warn("文件下载后重命名失败：tempPath:{},remotePath:{},handle file path:{}",localFilePath,remoteFile,downloadFileMetrics.getLocalFilePath());
            removeDownTrace(fileId);
        }
        else{
            if(ftpContext.isDeleteRemoteFile()) {
                try {

                    remoteFileAction.deleteFile(remoteFile);
                    if(logger.isDebugEnabled()){
                        logger.debug("删除远程ftp服务器文件{}完毕",remoteFile);
                    }
                } catch (Exception e) {
                    if(logger.isWarnEnabled())
                        logger.warn("删除远程ftp服务器文件失败：" + remoteFile, e);
                }
            }
        }


              
    }



    public void checkFtpNewFile(JobFlowNodeExecuteContext jobFlowNodeExecuteContext,String relativeParentDir,String parentDir,FTPFile remoteResourceInfo,   final RemoteContext ftpContext, List<Future> downloadFutures) {
        String name = remoteResourceInfo.getName().trim();

        String remoteFile = SimpleStringUtil.getPath(parentDir,name);
        checkRemoteNewFile(   jobFlowNodeExecuteContext,relativeParentDir,name, remoteFile,   ftpContext, new RemoteFileAction() {
            @Override
            public boolean downloadFile(String localFile,String remoteFile) {
                FtpTransfer.downloadFile((FtpContext) ftpContext,localFile,remoteFile);
                return true;
            }

            @Override
            public void deleteFile(String remoteFile) {
                FtpTransfer.deleteFile((FtpContext) ftpContext,remoteFile);
            }
        }, downloadFutures);


    }


 
}
