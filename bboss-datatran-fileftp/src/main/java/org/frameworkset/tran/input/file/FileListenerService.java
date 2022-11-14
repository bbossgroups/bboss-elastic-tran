package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.apache.commons.net.ftp.FTPFile;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.ftp.*;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.plugin.file.input.FileInputDataTranPlugin;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
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
    private Map<String,Object> downLoadTraces ;
    private Map<String, FileReaderTask> completedTasks;
    private Map<String, FileReaderTask> oldedTasks;
    private ImportContext importContext;
    private FileInputConfig fileInputConfig;
    private FileInputDataTranPlugin fileInputDataTranPlugin;
    private DataTranPlugin dataTranPlugin;
    private Object dummy = new Object();
    private Lock lock = new ReentrantLock();
    public FileListenerService(ImportContext fileImportContext) {
        this.fileConfigMap = new HashMap<String, FileReaderTask>();
        this.completedTasks = new HashMap<String, FileReaderTask>();
        this.oldedTasks = new HashMap<String, FileReaderTask>();
        downLoadTraces = new HashMap<>();
        this.importContext = fileImportContext;
        this.fileInputConfig = (FileInputConfig) importContext.getInputConfig();
        this.dataTranPlugin = fileImportContext.getDataTranPlugin();
        this.fileInputDataTranPlugin = (FileInputDataTranPlugin) dataTranPlugin.getInputPlugin();


    }


    public ImportContext getImportContext() {
        return importContext;
    }

    public void init(){
    	List<FileConfig> fileConfigList = fileInputConfig.getFileConfigList();
    	if(fileConfigList != null && fileConfigList.size() > 0){
    		for(int i = 0; i < fileConfigList.size(); i ++){
    			FileConfig fileConfig = fileConfigList.get(i);
    			fileConfig.init();
			}
		}
//        remoteFileChannel.init();
    }

    public boolean isSchedulePaussed(boolean autoPause){
        return this.dataTranPlugin.isSchedulePaussed(  autoPause);

    }
    public void moveTaskToComplete(FileReaderTask fileReaderTask){
        lock.lock();
        try {

            fileConfigMap.remove(fileReaderTask.getFileId());
            this.completedTasks.put(fileReaderTask.getFileId(), fileReaderTask);

        }
        finally {
            lock.unlock();
        }
        this.dataTranPlugin.afterCall(fileReaderTask.getTaskContext());
        fileReaderTask.destroyTaskContext();
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
        lock.lock();
        try {

            completedTasks.put(fileId,fileReaderTask);
        }
        finally {
            lock.unlock();
        }

    }

    public void addOldedFileTask(String fileId,FileReaderTask fileReaderTask){
        lock.lock();
        try {

            oldedTasks.put(fileId,fileReaderTask);
        }
        finally {
            lock.unlock();
        }

    }
    public void addFileTask(String fileId,FileReaderTask fileReaderTask){
        lock.lock();
        try {

            fileConfigMap.put(fileId,fileReaderTask);
        }
        finally {
            lock.unlock();
        }

    }


    //文件删除linux环境 文件删除了确实是文件删除了
    //window 环境无法判断 直接remove调
    public  void doDelete(String fileId) {
        lock.lock();
        try {

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
    public FileInputConfig getFileInputConfig() {
        return fileInputConfig;
    }

    public void checkTranFinished() {
        Map<String,FileReaderTask> copyFileConfigMap = null;
        lock.lock();
        try {


            copyFileConfigMap = new HashMap<String, FileReaderTask>();
            copyFileConfigMap.putAll(fileConfigMap);

        }
        finally {
            lock.unlock();
        }
		if(copyFileConfigMap != null && copyFileConfigMap.size() > 0) {
			Iterator<Map.Entry<String, FileReaderTask>> iterable = copyFileConfigMap.entrySet().iterator();

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


    }

    public void checkNewFile(String relativeParentDir,File file,FileConfig fileConfig) {

        String fileId = FileInodeHandler.inode(file,fileConfig.isEnableInode());
        lock.lock();
        try {

            if(this.dataTranPlugin.isPluginStopAppending())
                return;
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
                boolean successed = fileInputDataTranPlugin.initFileTask( relativeParentDir,fileConfig, currentStatus, file, pointer);
            }

        } finally {

            lock.unlock();
        }
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
    interface DownAction{
        void down(int times,boolean redown);
    }

    private RemoteFileValidate.Result remoteFileValidate(String remoteFile,File dataFile, FtpContext ftpContext, RemoteFileAction remoteFileAction,DownAction downAction,boolean redown){
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

    private boolean checkFileIdIsNew(String fileId){
        FileReaderTask fileReaderTask = fileConfigMap.get(fileId);
        boolean isNewFile = false;
        if (fileReaderTask == null) {
            if(this.completedTasks.containsKey(fileId)) {//作业已经完成
                logger.debug("Ignore complete file {}",fileId);

            }
            else if(this.oldedTasks.containsKey(fileId)){//作业已经被移除到过时作业清单
                logger.debug("Ignore old file {}",fileId);
            }
            else {
                isNewFile = true;
            }

        }
        return isNewFile;
    }
    /**
     *
     * @param relativeParentDir
     * @param fileName 远程文件名称
     * @param remoteFile  远程根目录+relativeParentDir
     * @param ftpContext
     * @param remoteFileAction
     */
    private void checkRemoteNewFile(final String relativeParentDir,String fileName, final String remoteFile,
                                    final FtpContext ftpContext, final RemoteFileAction remoteFileAction) {
        FtpConfig ftpConfig = ftpContext.getFtpConfig();
        FileConfig fileConfig = ftpContext.getFileConfig();
        final File handleFile = new File( SimpleStringUtil.getPath(fileConfig.getSourcePath(),relativeParentDir), fileName);//正式文件,如果有子目录，则需要保存到子目录
        checkParentExist(handleFile);
        final File localFile = new File(SimpleStringUtil.getPath(ftpConfig.getDownloadTempDir(),relativeParentDir),fileName);//临时下载文件，,如果有子目录，则需要保存到临时子目录，下载完毕后重命名为正式文件，如果正式文件不存在，需重新下载文件
        checkParentExist(localFile);
        final String fileId = FileInodeHandler.change(handleFile.getAbsolutePath());//ftp下载的文件直接使用文件路径作为fileId
        boolean isNewFile = false;
        boolean isDowning = false;
        lock.lock();
        try {

            if(this.dataTranPlugin.isPluginStopAppending() )
                return;
            isNewFile = checkFileIdIsNew(fileId);
            if(!downLoadTraces.containsKey(fileId)){
                downLoadTraces.put(fileId,dummy);
            }
            else{
                isDowning = true;
            }



        } finally {

            lock.unlock();
        }
        if(isNewFile && !isDowning) {
			RemoteFileChannel remoteFileChannel = ftpConfig.getRemoteFileChannel();
        	if(remoteFileChannel != null) { //异步并行下载文件
				remoteFileChannel.submitNewTask(new Runnable() {
					@Override
					public void run() {
					    try {
                            downAndCollectFile(handleFile, localFile, fileId,
                                    relativeParentDir, remoteFile, ftpContext, remoteFileAction);
                        }
					    catch (Exception e){
					        logger.error("Download remoteFile" + remoteFile+ " to " + localFile+ " And Collect File failed：",e);
                        }
					}
				});
			}
        	else{ //同步串行下载文件
				downAndCollectFile(handleFile, localFile, fileId,
						relativeParentDir, remoteFile, ftpContext, remoteFileAction);
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

    private void downAndCollectFile( File handleFile,File localFile,String fileId ,
                                    String relativeParentDir, final String remoteFile, FtpContext ftpContext, final RemoteFileAction remoteFileAction){
        FileConfig fileConfig = ftpContext.getFileConfig();
        /**
         * 如果处理文件不存在，则下载文件到本地临时目录,下载后重命名为正式处理文件，避免因下载中断处理不完整文件问题
         */
        if(!handleFile.exists()) {

            try {
                /**
                 * 支持断点续传
                 */
//                    SFTPTransfer.downloadFile(ftpContext,remoteFile,fileConfig.getDownloadTempDir());
                DownAction downAction = new DownAction() {
                    @Override
                    public void down(int times, boolean redown) {
                        if (redown) {
                            logger.warn("第{}次重试下载文件：localPath:{},remotePath:{}", times + 1, localFile.getAbsolutePath(), remoteFile);
                        }
                        remoteFileAction.downloadFile(localFile.getAbsolutePath(), remoteFile);
                        if (!localFile.exists()) {
                            if (!redown) {
                                logger.warn("下载文件失败：localPath:{},remotePath:{}", localFile.getAbsolutePath(), remoteFile);
                            } else {
                                logger.warn("第{}次重试下载文件失败：localPath:{},remotePath:{}", times + 1, localFile.getAbsolutePath(), remoteFile);
                            }

                        }
                    }
                };
//                    remoteFileAction.downloadFile(localFile.getAbsolutePath(),remoteFile);
                downAction.down(-1, false);
                if (!localFile.exists()) {
                    removeDownTrace(fileId);
                    return;
                }

                if(ftpContext.getRemoteFileValidate() != null) {
                    long startTime = System.currentTimeMillis();
                    RemoteFileValidate.Result result = remoteFileValidate(remoteFile, localFile, ftpContext, remoteFileAction, downAction, false);
                    long endTime = System.currentTimeMillis();
                    if (!result.isOk()) {
                        logger.warn("文件校验失败：localPath:{},remotePath:{},校验耗时：{}毫秒，失败原因：{}", localFile.getAbsolutePath(), remoteFile, endTime - startTime,result.getMessage());
                        removeDownTrace(fileId);
                        return;
                    }
                    else{
                        logger.info("文件校验成功：localPath:{},remotePath:{},校验耗时：{}毫秒", localFile.getAbsolutePath(), remoteFile, endTime - startTime);
                    }
                }
                boolean renamesuccess = localFile.renameTo(handleFile);
                if (renamesuccess) {
                    if (logger.isInfoEnabled())
                        logger.info("Rename " + localFile.getAbsolutePath() + " to " + handleFile.getAbsolutePath());
                } else {
                    removeDownTrace(fileId);
                    logger.warn("文件下载后重命名失败：tempPath:{},remotePath:{},handle file path:{}", localFile.getAbsolutePath(), remoteFile, handleFile.getAbsolutePath());
                    return;
                }
            }
            catch (Exception e){
                removeDownTrace(fileId);
                throw new FileDownException("下载文件"+remoteFile + "到"+handleFile.getAbsolutePath()+"失败",e);
            }
            catch (Throwable e){
                removeDownTrace(fileId);
                throw new FileDownException("下载文件"+remoteFile + "到"+handleFile.getAbsolutePath()+"失败",e);
            }
        }

        if(!handleFile.exists()){
            logger.warn("文件下载后重命名失败：tempPath:{},remotePath:{},handle file path:{}",localFile.getAbsolutePath(),remoteFile,handleFile.getAbsolutePath());
            removeDownTrace(fileId);
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
        lock.lock();
        try {

            //检查作业是否已经停止
            if (this.dataTranPlugin.isPluginStopAppending() )
                return;
            //再次检查文件是否已经被采集
            boolean isNewFile = checkFileIdIsNew(fileId);
            if(isNewFile) {
                if (logger.isInfoEnabled())
                    logger.info("Start collect new remote file {}", fileId);
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
                fileInputDataTranPlugin.initFileTask(relativeParentDir, fileConfig, currentStatus, handleFile, pointer);
            }
        }
        finally {
            lock.unlock();
        }
    }



    public void checkFtpNewFile(String relativeParentDir,String parentDir,FTPFile remoteResourceInfo,   final FtpContext ftpContext) {
        String name = remoteResourceInfo.getName().trim();

        String remoteFile = SimpleStringUtil.getPath(parentDir,name);
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


    }

    public void stopWorks(){
        Map temp = new LinkedHashMap();
        lock.lock();
        try{

            temp.putAll(fileConfigMap);
        }
        finally {
            lock.unlock();
        }
        if(temp.size() == 0)
        {
            return;
        }
        Iterator<Map.Entry<String,FileReaderTask>> iterator = temp.entrySet().iterator();

        while (iterator.hasNext()){
            Map.Entry<String,FileReaderTask> entry = iterator.next();
            entry.getValue().interruptWork();
        }

    }

    public DataTranPlugin getBaseDataTranPlugin() {
        return dataTranPlugin;
    }
}
