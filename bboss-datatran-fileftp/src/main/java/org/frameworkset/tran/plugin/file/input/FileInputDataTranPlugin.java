package org.frameworkset.tran.plugin.file.input;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.file.monitor.FileManager;
import org.frameworkset.tran.ftp.BackupSuccessFilesClean;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.input.file.*;
import org.frameworkset.tran.plugin.BaseInputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public class FileInputDataTranPlugin extends BaseInputPlugin {
    private static Logger logger = LoggerFactory.getLogger(FileInputDataTranPlugin.class);
    protected FileInputConfig fileInputConfig;
    protected List<LogDirScan> logDirScans;
    protected FileListenerService fileListenerService;
    protected LogDirsScanThread logDirsScanThread ;
    private BackupSuccessFilesClean backupSuccessFilesClean;
    private CompleteFileCleanTask completeFileCleanTask;
//    protected FileListener fileListener;
//    protected List<FileAlterationObserver> observerList = new ArrayList<FileAlterationObserver>();
    public FileInputDataTranPlugin(ImportContext importContext) {
        super(importContext);
        this.fileInputConfig = (FileInputConfig) importContext.getInputConfig();
    }
    @Override
    public boolean isEnablePluginTaskIntercept(){
        return false;
    }



    public boolean isMultiTran(){
        return true;
    }
//    public void setFileListener(FileListener fileListener) {
//        this.fileListener = fileListener;
//    }

    public void setFileListenerService(FileListenerService fileListenerService) {
        this.fileListenerService = fileListenerService;
    }

    public FileListenerService getFileListenerService() {
        return fileListenerService;
    }


    public Status getCurrentStatus(){
        throw new UnsupportedOperationException("getCurrentStatus");
    }
    public FileConfig getFileConfig(String filePath) {
        filePath = FileInodeHandler.change(filePath);
        List<FileConfig> list = fileInputConfig.getFileConfigList();
        FileConfig fileConfig = null;
        for(FileConfig config : list){
            if(config.checkFilePath(filePath)) {
                fileConfig = config;
                break;
            }
        }
        return fileConfig;
    }

    protected FileReaderTask buildFileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
												 Status currentStatus , FileInputConfig fileImportConfig ){
        FileReaderTask task = fileImportConfig.buildFileReaderTask(taskContext,file,fileId,fileConfig,pointer,
                fileListenerService,fileDataTran,currentStatus,fileImportConfig);
        return task;
    }
    protected FileReaderTask buildFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig ){
        FileReaderTask task =  fileImportConfig.buildFileReaderTask(fileId,currentStatus,fileImportConfig);
        return task;
    }
    public FileTaskContext createFileTaskContext(Status status,FileConfig fileConfig){
        final FileTaskContext taskContext = new FileTaskContext(importContext);
        //构建文件信息
        File file = new File(status.getRealPath());
        String charSet = fileConfig.getCharsetEncode() ;
        if(charSet == null || charSet.equals("")){
            charSet = getFileListenerService().getFileInputConfig().getCharsetEncode();
        }
        FileInfo fileInfo = new FileInfo(charSet,
                FileInodeHandler.change(file.getAbsolutePath()),
                file,  status.getFileId(), fileConfig);
        fileInfo.setCloseEOF(fileConfig.isCloseEOF());
        taskContext.setFileInfo(fileInfo);
        return taskContext;
    }
    public boolean initFileTask(String relativeParentDir,FileConfig fileConfig,Status status,File file,long pointer){

        if(fileConfig == null){
            return false;
        }
        dataTranPlugin.addStatus( status);
        FileResultSet kafkaResultSet = new FileResultSet(this.importContext);
//		final CountDownLatch countDownLatch = new CountDownLatch(1);
//        FileTaskContext taskContext = new FileTaskContext(importContext);
        FileTaskContext taskContext = createFileTaskContext(status,fileConfig);
        dataTranPlugin.preCall(taskContext);//需要在任务完成时销毁taskContext
        final BaseDataTran fileDataTran = dataTranPlugin.createBaseDataTran(taskContext,kafkaResultSet,null,status);
        Thread tranThread = null;
        try {
            if(fileDataTran != null) {
                String fileId = FileInodeHandler.inode(file,fileConfig.isEnableInode());
                String tname = "file-log-tran|"+status.getRealPath();
                if(fileConfig.isEnableInode()){
                    tname = tname + "|" +fileId;
                }
                tranThread = new Thread(new Runnable() {
                    @Override
                    public void run() {

                        try {
                            fileDataTran.tran();
                        }
                        catch (DataImportException dataImportException){
                            logger.error("",dataImportException);
                            dataTranPlugin.throwException(  taskContext,  dataImportException);
                        }
                        catch (RuntimeException dataImportException){
                            logger.error("",dataImportException);
                            dataTranPlugin.throwException(  taskContext,  dataImportException);
                        }
                        catch (Throwable dataImportException){
                            logger.error("",dataImportException);
                            DataImportException dataImportException_ = new DataImportException(dataImportException);
                            dataTranPlugin.throwException(  taskContext, dataImportException_);
                        }
                    }
                }, tname);
                tranThread.start();
                if(logger.isInfoEnabled())
                    logger.info(tname+" started.");


                FileReaderTask task = buildFileReaderTask(taskContext,file,fileId,fileConfig,pointer,
                        fileListenerService,fileDataTran,status,fileInputConfig);
                taskContext.setFileInfo(task.getFileInfo());
                if(fileConfig.getAddFields() != null && fileConfig.getAddFields().size() > 0){
                    task.addFields(fileConfig.getAddFields());
                }
                if(fileConfig.getIgnoreFields() != null && fileConfig.getIgnoreFields().size() > 0){
                    task.ignoreFields(fileConfig.getIgnoreFields());
                }
                /**
                 * 根据文件信息动态添加文件标签
                 */
                if(fileConfig.getFieldBuilder() != null){
                    fileConfig.getFieldBuilder().buildFields(task.getFileInfo(),task);
                }

//                fileConfigMap.put(fileId,task);
                fileListenerService.addFileTask(fileId,task);
                task.start();
            }
            return true;
        } catch (DataImportException e) {
            dataTranPlugin.throwException(taskContext,e);
            throw e;
        } catch (Exception e) {
            dataTranPlugin.throwException(taskContext,e);
            throw new DataImportException(e);
        }

    }
    private boolean isNeedClosed(Status status,FileConfig fileConfig){
        Long oldedTime = fileConfig.getCloseOlderTime();
        if(oldedTime == null || oldedTime == 0)
            return false;
        long lastTime = status.getTime();

        long stopTime = System.currentTimeMillis() - oldedTime;

        if(lastTime <= stopTime){
            return true;
        }

        return false;
    }
    private boolean isOlded(Status status,FileConfig fileConfig){
        Long oldedTime = fileConfig.getIgnoreOlderTime();
        if(oldedTime == null || oldedTime == 0)
            return false;
        long lastTime = status.getTime();

        long stopTime = System.currentTimeMillis() - oldedTime;

        if(lastTime <= stopTime){
            return true;
        }

        return false;
    }


    private void stopScanThread(){

        if(logDirsScanThread != null){

            logger.info("StopScanThread:LogDirScanThread");
            try {
                logDirsScanThread.stop();
            } catch (Exception e) {

            }
//            logDirsScanThread = null;
        }
    }
    @Override
    public void destroy(boolean waitTranStop){

        fileListenerService.checkTranFinished();//检查所有的作业是否已经结束，并等待作业结束
        if(backupSuccessFilesClean != null) {
            this.backupSuccessFilesClean.stop();
        }
        if(completeFileCleanTask != null){
            completeFileCleanTask.stopThread();
		}
    }


    @Override
    public void stopCollectData() {
        stopScanThread();
        super.stopCollectData();
        //等待所有采集任务结束
        fileListenerService.stopWorks();
    }


    //    protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet,Status currentStatus);
    @Override
    public void beforeInit() {
//        initSourceDatasource();
        FileListenerService fileListenerService = new FileListenerService(importContext);
//		FileListener fileListener = new FileListener(new FileListenerService((FileImportContext) importContext,file2ESDataTranPlugin));
        fileListenerService.init();
        setFileListenerService(fileListenerService);
    }

    @Override
    public void init() {

    }


    @Override
    public void afterInit(){
        if (fileInputConfig != null) {
            if (fileInputConfig.isBackupSuccessFiles()) {
                synchronized (BackupSuccessFilesClean.class) {
                    if(backupSuccessFilesClean == null){
                        String backupSuccessFileDir = fileInputConfig.getBackupSuccessFileDir();
                        if (backupSuccessFileDir == null || backupSuccessFileDir.equals("")) {
                            logger.warn("开启了备份文件机制，但是没有指定备份目录，请检查fileInputConfig并设置backupSuccessFileDir");
                            throw new FilelogPluginException("开启了备份成功文件机制，但是没有指定备份目录，请检查fileInputConfig并设置backupSuccessFileDir");
                        } else {
                            boolean backupEnable = fileInputConfig.getBackupSuccessFileInterval() > 0L && fileInputConfig.getBackupSuccessFileLiveTime() > 0L;
                            if (backupEnable) {
                                logger.info("启动备份文件清理线程，BackupSuccessFileChekInterval[{}毫秒，失效备份文件扫描时间间隔]和BackupSuccessFileLiveTime[{}毫秒，备份文件存活时间]",fileInputConfig.getBackupSuccessFileInterval(),fileInputConfig.getBackupSuccessFileLiveTime());
                                backupSuccessFilesClean = new BackupSuccessFilesClean(fileInputConfig);
                                backupSuccessFilesClean.start();
                            }
                            else{
                                logger.warn("参数设置不正确：开启了备份成功文件机制，BackupSuccessFileChekInterval[{}毫秒，失效备份文件扫描时间间隔]和BackupSuccessFileLiveTime[{}毫秒，备份文件存活时间],二者都大于0时才能启用备份文件清理功能！",fileInputConfig.getBackupSuccessFileInterval(),fileInputConfig.getBackupSuccessFileLiveTime());
                                throw new FilelogPluginException("参数设置不正确：开启了备份成功文件机制，BackupSuccessFileChekInterval["+fileInputConfig.getBackupSuccessFileInterval()+"毫秒，失效备份文件扫描时间间隔]和BackupSuccessFileLiveTime["+fileInputConfig.getBackupSuccessFileLiveTime()+"毫秒，备份文件存活时间],二者都大于0时才能启用备份文件清理功能！");
                            }
                        }
                    }
                }
            }
            else if(fileInputConfig.isCleanCompleteFiles()){
                if(fileInputConfig.getFileLiveTime() <= 0L){
                    logger.warn("开启了清理采集完毕文件机制，但是没有正确设置FileLiveTime["+fileInputConfig.getFileLiveTime()+"]，必须指定一个大于0的文件存活时间，单位：毫秒，请检查fileInputConfig并设置FileLiveTime");
                    throw new FilelogPluginException("开启了清理采集完毕文件机制，但是没有正确设置FileLiveTime["+fileInputConfig.getFileLiveTime()+"]，必须指定一个大于0的文件存活时间，单位：毫秒，请检查fileInputConfig并设置FileLiveTime");
                }
                synchronized (CompleteFileCleanTask.class){
                    if(completeFileCleanTask == null){
                        completeFileCleanTask = new CompleteFileCleanTask(importContext);
                        completeFileCleanTask.start();
                        logger.info("开启清理采集完毕文件机制，采集完毕文件保留有效期:{}毫秒",fileInputConfig.getFileLiveTime());
                    }
                }
            }
        }

    }
    @Override
    public boolean isEnableAutoPauseScheduled(){
        return fileInputConfig.isEnableAutoPauseScheduled();
    }
    @Override
    public void initStatusTableId() {

    }


    private FtpConfig getFtpConfig(FileConfig fileConfig){
//        if(fileConfig instanceof FtpConfig)
//            return (FtpConfig)fileConfig;
        return fileConfig.getFtpConfig();

    }

    /**
     * 判断文件是否已经采集完毕并且已经过期，如果是则加入到过期清理清单
     * @param checkResult
     * @param file
     */
    public void handleCompleteFiles(int checkResult,File file){
        if(completeFileCleanTask != null) {
            if (checkResult == FileCheckResult.FileCheckResult_CompleteFile || checkResult == FileCheckResult.FileCheckResult_OldFile) {
                long lastModifyTime = FileManager.getFileLastTimestamp(file);
                long limit = System.currentTimeMillis() - fileInputConfig.getFileLiveTime();
                if (lastModifyTime <= limit) {
                    completeFileCleanTask.addFile(file);
                }
            }
        }

    }
    private LogDirScan logDirScanThread(LogDirsScanThread logDirsScanThread, FileConfig fileConfig ){
        LogDirScan logDirScan = null;
        FtpConfig ftpConfig = getFtpConfig(fileConfig);
        if (ftpConfig != null) {
//            FtpConfig ftpConfig = (FtpConfig) fileConfig;
            if(ftpConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
                logDirScan = new FtpLogDirScan(logDirsScanThread,
                        fileConfig, getFileListenerService());
                logDirScan.setRemote(true);
            }
            else{
                logDirScan = new SFtpLogDirScan(logDirsScanThread,
                        fileConfig, getFileListenerService());
                logDirScan.setRemote(true);
            }

        } else {
            logDirScan = new LogDirScan(logDirsScanThread,
                    fileConfig, getFileListenerService());
            logDirScan.setRemote(false);
        }
        return logDirScan;
    }
    @Override
    public void doImportData(TaskContext taskContext) throws DataImportException {

        if(fileInputConfig != null)
        {
            List<FileConfig> fileConfigs = this.fileInputConfig.getFileConfigList();
            if (fileConfigs != null && fileConfigs.size() > 0) {


                if (!fileInputConfig.isUseETLScheduleForScanNewFile()) {//采用内置新文件扫描调度机制
                    ScanNewFile scan = new ScanNewFile() {
                        @Override
                        public boolean run() {
                            boolean schedulePaused = fileListenerService.isSchedulePaussed(fileInputConfig.isEnableAutoPauseScheduled());
                            if(!schedulePaused) {
                                for (LogDirScan logDirScan : logDirScans) {
                                    try {
                                        logDirScan.scanNewFile();
                                    } catch (Exception e) {
                                        logger.error("扫描新文件异常:" + logDirScan.getFileConfig().toString(), e);
                                    }
                                }
                            }
                            else{
                                if(logger.isInfoEnabled()){
                                    logger.info("Ignore Scan new files for Paussed Schedule Task,waiting for next resume schedule sign to continue.");
                                }
                            }
                            return schedulePaused;
                        }
                    };
                    logDirsScanThread = new LogDirsScanThread(scan,fileInputConfig);
                    logDirScans = new ArrayList<>(fileConfigs.size());
                    for (FileConfig fileConfig : fileConfigs) {
                        //多个文件目录配置时，不能自动暂停，否则可以
                        LogDirScan logDirScan = logDirScanThread( logDirsScanThread, fileConfig);

                        logDirScans.add(logDirScan);
//                        logDirScanThread.start();
                    }

                    logDirsScanThread.start();


                } else {//采用外部新文件扫描调度机制：jdk timer,quartz,xxl-job

                    if(logDirScans == null){ //初始执行不判断是否调度暂停，后续需要进行判断
                        logDirsScanThread = new LogDirsScanThread(null,fileInputConfig);
                        logDirScans = new ArrayList<>(fileConfigs.size());
                        logDirsScanThread.statusRunning();
                        for (FileConfig fileConfig : fileConfigs) {
                            LogDirScan logDirScan = logDirScanThread(logDirsScanThread,  fileConfig);
                            logDirScans.add(logDirScan);
                            logDirScan.scanNewFile();
                        }

                    }
                    else{
//                        boolean schedulePaused = this.fileListenerService.isSchedulePaussed(true);
//                        if(!schedulePaused) {
//                            for (LogDirScanThread logDirScanThread : logDirScanThreads) {
//                                try {
//                                    logDirScanThread.scanNewFile();
//                                } catch (Exception e) {
//                                    logger.error("扫描新文件异常:" + logDirScanThread.getFileConfig().toString(), e);
//                                }
//                            }
//                        }
//                        else{
//                            if(logger.isInfoEnabled()){
//                                logger.info("Ignore  Paussed Schedule Task,waiting for next resume schedule sign to continue.");
//                            }
//                        }
                        for (LogDirScan logDirScan : logDirScans) {
                            try {
                                logDirScan.scanNewFile();
                            } catch (Exception e) {
                                logger.error("扫描新文件异常:" + logDirScan.getFileConfig().toString(), e);
                            }
                        }
                    }

                }
            }

        }

    }

}
