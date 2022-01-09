package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.apache.commons.net.ftp.FTPFile;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpContext;
import org.frameworkset.tran.ftp.FtpTransfer;
import org.frameworkset.tran.ftp.SFTPTransfer;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class FileListenerService {
    private static Logger logger = LoggerFactory.getLogger(FileListenerService.class);
    private Map<String, FileReaderTask> fileConfigMap;
    private Map<String, FileReaderTask> completedTasks;
    private Map<String, FileReaderTask> oldedTasks;
    private FileImportContext fileImportContext;
    private FileBaseDataTranPlugin baseDataTranPlugin;
    private Lock lock = new ReentrantLock();
    public FileListenerService(FileImportContext fileImportContext,FileBaseDataTranPlugin baseDataTranPlugin) {
        this.fileConfigMap = new HashMap<String, FileReaderTask>();
        this.completedTasks = new HashMap<String, FileReaderTask>();
        this.oldedTasks = new HashMap<String, FileReaderTask>();
        this.fileImportContext = fileImportContext;
        this.baseDataTranPlugin = baseDataTranPlugin;
    }

    public void moveTaskToComplete(FileReaderTask fileReaderTask){
        try {
            lock.lock();
            fileConfigMap.remove(fileReaderTask.getFileId());
            this.completedTasks.put(fileReaderTask.getFileId(), fileReaderTask);
            this.baseDataTranPlugin.afterCall(fileReaderTask.getTaskContext());
            fileReaderTask.destroyTaskContext();
        }
        finally {
            lock.unlock();
        }
    }

//    public void doChange(File file){
//        String fileId = FileInodeHandler.inode(file);
//        if(completedTasks.containsKey(fileId)){ // 已经采集过的文件直接返回
//            logger.info("file {} has complate collected." ,file.getAbsolutePath());
//            return;
//        }
//        if(oldedTasks.containsKey(fileId)){ // 已经采集过的文件直接返回
//            logger.info("file {} olded collected." ,file.getAbsolutePath());
//            return;
//        }
//        if(!fileConfigMap.containsKey(fileId) ){
//            FileConfig fileConfig = baseDataTranPlugin.getFileConfig(file.getAbsolutePath());
//            if(fileConfig == null)
//                return;
//            Status currentStatus = new Status();
//            currentStatus.setId(fileId.hashCode());
//            currentStatus.setTime(new Date().getTime());
//            currentStatus.setFileId(fileId);
//            currentStatus.setFilePath(FileInodeHandler.change(file.getAbsolutePath()));
//            currentStatus.setStatus(ImportIncreamentConfig.STATUS_COLLECTING);
//            long pointer = fileConfig.getStartPointer() !=null && fileConfig.getStartPointer() > 0l ?fileConfig.getStartPointer():0l;
//            currentStatus.setLastValue(pointer);
//
//            boolean successed = baseDataTranPlugin.initFileTask(fileConfig,currentStatus,file,pointer);
//
//
//
//        }else{
//
//            FileReaderTask task = fileConfigMap.get(fileId);
//            task.setFile(file);
//            task.dataChange();
//        }
//    }

    public void addCompletedFileTask(String fileId,FileReaderTask fileReaderTask){
        try {
            lock.lock();
            completedTasks.put(fileId,fileReaderTask);
        }
        finally {
            lock.unlock();
        }

    }

    public void addOldedFileTask(String fileId,FileReaderTask fileReaderTask){
        try {
            lock.lock();
            oldedTasks.put(fileId,fileReaderTask);
        }
        finally {
            lock.unlock();
        }

    }
    public void addFileTask(String fileId,FileReaderTask fileReaderTask){
        try {
            lock.lock();
            fileConfigMap.put(fileId,fileReaderTask);
        }
        finally {
            lock.unlock();
        }

    }


    //文件删除linux环境 文件删除了确实是文件删除了
    //window 环境无法判断 直接remove调
    public  void doDelete(String fileId) {
        try {
            lock.lock();
            FileReaderTask fileReaderTask = fileConfigMap.remove(fileId);
            /**
            if(fileReaderTask != null){
                this.completedTasks.put(fileId, fileReaderTask);

                fileReaderTask.taskEnded();
                final FileReaderTask fileReaderTask_ = fileReaderTask;
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sleep(5000l);//延迟5秒后存储状态
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        Status currentStatus = fileReaderTask_.getCurrentStatus();
                        baseDataTranPlugin.forceflushLastValue(currentStatus);
                        baseDataTranPlugin.afterCall(fileReaderTask_.getTaskContext());
                        fileReaderTask_.destroyTaskContext();
                    }
                });
                thread.start();

                // todo 删除文件状态更新
            }
             */
        }
        finally {
            lock.unlock();
        }


    }
//    //文件移动 linux环境才能根据inode判断文件移动了
//    public void onFileMove(File oldFile, File newFile) {
//        String fileId = FileInodeHandler.inode(newFile);
//        FileReaderTask fileReaderTask = null;
//        try {
//            lock.lock();
//            fileReaderTask = fileConfigMap.get(fileId);
//        }
//        finally {
//            lock.unlock();
//        }
//        //不存在的不处理，会有创建事件去处理了
//        if(fileReaderTask != null){
//            fileReaderTask.changeFile(newFile);
//            //文件移到不需要执行采集操作
////            fileReaderTask.execute();
//        }
//    }
    public FileImportContext getFileImportContext() {
        return fileImportContext;
    }

    public void checkTranFinished() {
        try {
            lock.lock();
            Iterator<Map.Entry<String, FileReaderTask>> iterable = fileConfigMap.entrySet().iterator();

            while (iterable.hasNext()) {
                Map.Entry<String, FileReaderTask> entry = iterable.next();
                BaseDataTran baseDataTran = entry.getValue().getFileDataTran();
                if (baseDataTran.isTranFinished()) {
                    continue;
                } else {
                    do {
                        if (!baseDataTran.isTranFinished()) {
                            try {
                                sleep(500);
                            } catch (InterruptedException e) {
                                break;
                            }
                        } else {
                            break;
                        }
                    } while (true);
                }
            }
        }
        finally {
            lock.unlock();
        }

    }

    public void checkNewFile(String relativeParentDir,File file,FileConfig fileConfig) {

        String fileId = FileInodeHandler.inode(file,fileConfig.isEnableInode());
        try {
            lock.lock();
            FileReaderTask fileReaderTask = fileConfigMap.get(fileId);

            if (fileReaderTask == null) {
                if(this.completedTasks.containsKey(fileId)) {//作业已经完成
                    logger.debug("Ignore complete file {}",file.getAbsolutePath());
                    return;
                }
                if(this.oldedTasks.containsKey(fileId)){//作业已经被移除到过时作业清单
                    logger.debug("Ignore old file {}",file.getAbsolutePath());
                    return;
                }

                //创建新的采集任务

                if(logger.isInfoEnabled())
                    logger.info("Start collect new log file {}",file.getAbsolutePath());
                Status currentStatus = new Status();
                currentStatus.setId(fileId.hashCode());
                currentStatus.setTime(new Date().getTime());
                currentStatus.setFileId(fileId);
                currentStatus.setRelativeParentDir(relativeParentDir);
                currentStatus.setFilePath(FileInodeHandler.change(file.getAbsolutePath()));
                currentStatus.setRealPath(currentStatus.getFilePath());
                currentStatus.setStatus(ImportIncreamentConfig.STATUS_COLLECTING);
                long pointer = fileConfig.getStartPointer() != null && fileConfig.getStartPointer() > 0l ? fileConfig.getStartPointer() : 0l;
                currentStatus.setLastValue(pointer);
                boolean successed = baseDataTranPlugin.initFileTask( relativeParentDir,fileConfig, currentStatus, file, pointer);
            }

        } finally {

            lock.unlock();
        }
    }


    static interface RemoteFileAction {
        boolean downloadFile(String localFile,String remoteFile);
        void deleteFile(String remoteFile);
    }

    public void checkSFtpNewFile(String relativeParentDir,RemoteResourceInfo remoteResourceInfo,  final FtpContext ftpContext) {
        checkRemoteNewFile( relativeParentDir,remoteResourceInfo.getName(), remoteResourceInfo.getPath(),  ftpContext, new RemoteFileAction() {
            @Override
            public boolean downloadFile(String localFile,String remoteFile) {
                SFTPTransfer.downloadFile(ftpContext,remoteFile,localFile);
                return true;
            }

            @Override
            public void deleteFile(String remoteFile) {
                  SFTPTransfer.deleteFile(ftpContext,remoteFile);
            }
        });
//        File handleFile = new File( fileConfig.getSourcePath(), remoteResourceInfo.getName());//正式文件
//        File localFile = new File(fileConfig.getDownloadTempDir(),remoteResourceInfo.getName());//临时下载文件，下载完毕后重命名为正式文件，如果正式文件不存在，需重新下载文件
//
//        String fileId = FileInodeHandler.change(handleFile.getAbsolutePath());//ftp下载的文件直接使用文件路径作为fileId
//        try {
//            lock.lock();
//            FileReaderTask fileReaderTask = fileConfigMap.get(fileId);
//
//            if (fileReaderTask == null) {
//                if(this.completedTasks.containsKey(fileId)) {//作业已经完成
//                    logger.debug("Ignore complete file {}",fileId);
//                    return;
//                }
//                if(this.oldedTasks.containsKey(fileId)){//作业已经被移除到过时作业清单
//                    logger.debug("Ignore old file {}",fileId);
//                    return;
//                }
//                /**
//                 * 如果处理文件不存在，则下载文件到本地临时目录,下载后重命名为正式处理文件，避免因下载中断处理不完整文件问题
//                 */
//                if(!handleFile.exists()){
//
//                    /**
//                     * 支持断点续传
//                     */
//                    SFTPTransfer.downloadFile(ftpContext,remoteResourceInfo.getPath(),fileConfig.getDownloadTempDir());
//                    if(!localFile.exists()){
//                        logger.warn("文件下载失败：localPath:{},remotePath:{}",localFile.getAbsolutePath(),remoteResourceInfo.getPath());
//                        return;
//                    }
//                    localFile.renameTo(handleFile);
//                }
//                if(!handleFile.exists()){
//                    logger.warn("文件下载后重命名失败：tempPath:{},remotePath:{},handle file path:{}",localFile.getAbsolutePath(),remoteResourceInfo.getPath(),handleFile.getAbsolutePath());
//                    return;
//                }
//                //创建新的采集任务
//
//                if(logger.isInfoEnabled())
//                    logger.info("Start collect new ftp file {}",fileId);
//                Status currentStatus = new Status();
//                currentStatus.setId(fileId.hashCode());
//                currentStatus.setTime(new Date().getTime());
//                currentStatus.setFileId(fileId);
//                currentStatus.setFilePath(fileId);
//                currentStatus.setRealPath(fileId);
//                currentStatus.setStatus(ImportIncreamentConfig.STATUS_COLLECTING);
//                long pointer = fileConfig.getStartPointer() != null && fileConfig.getStartPointer() > 0l ? fileConfig.getStartPointer() : 0l;
//                currentStatus.setLastValue(pointer);
//                boolean successed = baseDataTranPlugin.initFileTask(fileConfig, currentStatus, handleFile, pointer);
//            }
//
//        } finally {
//
//            lock.unlock();
//        }

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
    private void checkRemoteNewFile(String relativeParentDir,String fileName, String remoteFile, FtpContext ftpContext, RemoteFileAction remoteFileAction) {
        FtpConfig fileConfig = ftpContext.getFtpConfig();
        File handleFile = new File( SimpleStringUtil.getPath(fileConfig.getSourcePath(),relativeParentDir), fileName);//正式文件,如果有子目录，则需要保存到子目录
        checkParentExist(handleFile);
        File localFile = new File(SimpleStringUtil.getPath(fileConfig.getDownloadTempDir(),relativeParentDir),fileName);//临时下载文件，,如果有子目录，则需要保存到临时子目录，下载完毕后重命名为正式文件，如果正式文件不存在，需重新下载文件
        checkParentExist(localFile);
        String fileId = FileInodeHandler.change(handleFile.getAbsolutePath());//ftp下载的文件直接使用文件路径作为fileId
        try {
            lock.lock();
            FileReaderTask fileReaderTask = fileConfigMap.get(fileId);

            if (fileReaderTask == null) {
                if(this.completedTasks.containsKey(fileId)) {//作业已经完成
                    logger.debug("Ignore complete file {}",fileId);
                    return;
                }
                if(this.oldedTasks.containsKey(fileId)){//作业已经被移除到过时作业清单
                    logger.debug("Ignore old file {}",fileId);
                    return;
                }
                /**
                 * 如果处理文件不存在，则下载文件到本地临时目录,下载后重命名为正式处理文件，避免因下载中断处理不完整文件问题
                 */
                if(!handleFile.exists()){

                    /**
                     * 支持断点续传
                     */
//                    SFTPTransfer.downloadFile(ftpContext,remoteFile,fileConfig.getDownloadTempDir());
                    remoteFileAction.downloadFile(localFile.getAbsolutePath(),remoteFile);
                    if(!localFile.exists()){
                        logger.warn("文件下载失败：localPath:{},remotePath:{}",localFile.getAbsolutePath(),remoteFile);
                        return;
                    }
                    localFile.renameTo(handleFile);
                }
                if(!handleFile.exists()){
                    logger.warn("文件下载后重命名失败：tempPath:{},remotePath:{},handle file path:{}",localFile.getAbsolutePath(),remoteFile,handleFile.getAbsolutePath());
                    return;
                }
                else{
                    if(ftpContext.deleteRemoteFile()) {
                        try {

                            remoteFileAction.deleteFile(remoteFile);
                            if(logger.isDebugEnabled()){
                                logger.debug("删除远程ftp服务器文件{}完毕",remoteFile);
                            }
                        } catch (Exception e) {
                            logger.warn("删除远程ftp服务器文件失败：" + remoteFile, e);
                        }
                    }
                }
                //创建新的采集任务

                if(logger.isInfoEnabled())
                    logger.info("Start collect new remote file {}",fileId);
                Status currentStatus = new Status();
                currentStatus.setId(fileId.hashCode());
                currentStatus.setTime(new Date().getTime());
                currentStatus.setFileId(fileId);
                currentStatus.setFilePath(fileId);
                currentStatus.setRealPath(fileId);
                currentStatus.setRelativeParentDir(relativeParentDir);
                currentStatus.setStatus(ImportIncreamentConfig.STATUS_COLLECTING);
                long pointer = fileConfig.getStartPointer() != null && fileConfig.getStartPointer() > 0l ? fileConfig.getStartPointer() : 0l;
                currentStatus.setLastValue(pointer);
                boolean successed = baseDataTranPlugin.initFileTask( relativeParentDir,fileConfig, currentStatus, handleFile, pointer);
            }

        } finally {

            lock.unlock();
        }

    }


    public void checkFtpNewFile(String relativeParentDir,FTPFile remoteResourceInfo,   final FtpContext ftpContext) {
        String name = remoteResourceInfo.getName().trim();
        String remoteFile = SimpleStringUtil.getPath(ftpContext.getRemoteFileDir(),name);
        checkRemoteNewFile( relativeParentDir,name, remoteFile,   ftpContext, new RemoteFileAction() {
            @Override
            public boolean downloadFile(String localFile,String remoteFile) {
                FtpTransfer.downloadFile(ftpContext,localFile,remoteFile);
                return true;
            }

            @Override
            public void deleteFile(String remoteFile) {
                FtpTransfer.deleteFile(ftpContext,remoteFile);
            }
        });
//        File handleFile = new File( fileConfig.getSourcePath(), remoteResourceInfo.getName());//正式文件
//        File localFile = new File(fileConfig.getDownloadTempDir(),remoteResourceInfo.getName());//临时下载文件，下载完毕后重命名为正式文件，如果正式文件不存在，需重新下载文件
//
//        String fileId = FileInodeHandler.change(handleFile.getAbsolutePath());//ftp下载的文件直接使用文件路径作为fileId
//        try {
//            lock.lock();
//            FileReaderTask fileReaderTask = fileConfigMap.get(fileId);
//
//            if (fileReaderTask == null) {
//                if(this.completedTasks.containsKey(fileId)) {//作业已经完成
//                    logger.debug("Ignore complete file {}",fileId);
//                    return;
//                }
//                if(this.oldedTasks.containsKey(fileId)){//作业已经被移除到过时作业清单
//                    logger.debug("Ignore old file {}",fileId);
//                    return;
//                }
//                String remoteFile = SimpleStringUtil.getPath(ftpContext.getRemoteFileDir(),remoteResourceInfo.getName());
//                /**
//                 * 如果处理文件不存在，则下载文件到本地临时目录,下载后重命名为正式处理文件，避免因下载中断处理不完整文件问题
//                 */
//                if(!handleFile.exists()){
//
//                    /**
//                     * 支持断点续传
//                     */
//
//                    FtpTransfer.downloadFile(ftpContext,localFile.getAbsolutePath(),remoteFile);
//                    if(!localFile.exists()){
//                        logger.warn("文件下载失败：localPath:{},remotePath:{}",localFile.getAbsolutePath(),remoteFile);
//                        return;
//                    }
//                    localFile.renameTo(handleFile);
//                }
//                if(!handleFile.exists()){
//                    logger.warn("文件下载后重命名失败：tempPath:{},remotePath:{},handle file path:{}",localFile.getAbsolutePath(),remoteFile,handleFile.getAbsolutePath());
//                    return;
//                }
//                //创建新的采集任务
//
//                if(logger.isInfoEnabled())
//                    logger.info("Start collect new ftp file {}",fileId);
//                Status currentStatus = new Status();
//                currentStatus.setId(fileId.hashCode());
//                currentStatus.setTime(new Date().getTime());
//                currentStatus.setFileId(fileId);
//                currentStatus.setFilePath(fileId);
//                currentStatus.setRealPath(fileId);
//                currentStatus.setStatus(ImportIncreamentConfig.STATUS_COLLECTING);
//                long pointer = fileConfig.getStartPointer() != null && fileConfig.getStartPointer() > 0l ? fileConfig.getStartPointer() : 0l;
//                currentStatus.setLastValue(pointer);
//                boolean successed = baseDataTranPlugin.initFileTask(fileConfig, currentStatus, handleFile, pointer);
//            }
//
//        } finally {
//
//            lock.unlock();
//        }

    }

    public FileBaseDataTranPlugin getBaseDataTranPlugin() {
        return baseDataTranPlugin;
    }
}
