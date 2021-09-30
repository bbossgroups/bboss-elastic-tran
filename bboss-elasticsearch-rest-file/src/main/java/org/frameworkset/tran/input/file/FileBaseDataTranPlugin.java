package org.frameworkset.tran.input.file;

import com.frameworkset.common.poolman.SQLExecutor;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.BaseDataTranPlugin;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.ftp.BackupSuccessFilesClean;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.MultiStatusManager;
import org.frameworkset.tran.util.TranConstant;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public abstract class FileBaseDataTranPlugin extends BaseDataTranPlugin {
    protected FileImportContext fileImportContext;
    protected List<LogDirScanThread> logDirScanThreads;
    protected FileListenerService fileListenerService;

    private static BackupSuccessFilesClean backupSuccessFilesClean;
//    protected FileListener fileListener;
//    protected List<FileAlterationObserver> observerList = new ArrayList<FileAlterationObserver>();
    public FileBaseDataTranPlugin(ImportContext importContext,
                                  ImportContext targetImportContext) {
        super(importContext, targetImportContext);
        this.fileImportContext = (FileImportContext) importContext;

    }
    public boolean isMultiTran(){
        return true;
    }
//    public void setFileListener(FileListener fileListener) {
//        this.fileListener = fileListener;
//    }
    @Override
    protected void initStatusManager(){
        statusManager = new MultiStatusManager(statusDbname, updateSQL, lastValueType,this);
        statusManager.init();
    }
    public void setFileListenerService(FileListenerService fileListenerService) {
        this.fileListenerService = fileListenerService;
    }

    public FileListenerService getFileListenerService() {
        return fileListenerService;
    }

    @Override
    public void initLastValueClumnName(){

    }
    public Status getCurrentStatus(){
        throw new UnsupportedOperationException("getCurrentStatus");
    }
    public FileConfig getFileConfig(String filePath) {
        filePath = FileInodeHandler.change(filePath);
        List<FileConfig> list = fileImportContext.getFileConfigList();
        FileConfig fileConfig = null;
        for(FileConfig config : list){
            if(config.checkFilePath(filePath)) {
                fileConfig = config;
                break;
            }
        }
        return fileConfig;
    }

    public boolean initFileTask(FileConfig fileConfig,Status status,File file,long pointer){

        if(fileConfig == null){
            return false;
        }
        addStatus( status);
        FileResultSet kafkaResultSet = new FileResultSet(this.fileImportContext);
//		final CountDownLatch countDownLatch = new CountDownLatch(1);
        FileTaskContext taskContext = new FileTaskContext(importContext,targetImportContext);

        final BaseDataTran fileDataTran = createBaseDataTran(taskContext,kafkaResultSet,status);
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
                        fileDataTran.tran();
                    }
                }, tname);
                tranThread.start();
                if(logger.isInfoEnabled())
                    logger.info(tname+" started.");


                FileReaderTask task = new FileReaderTask(taskContext,file,fileId,fileConfig,pointer,
                        fileListenerService,fileDataTran,status,fileImportContext.getFileImportConfig());
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
                preCall(taskContext);//需要在任务完成时销毁taskContext
//                fileConfigMap.put(fileId,task);
                fileListenerService.addFileTask(fileId,task);
                task.start();
            }
            return true;
        } catch (ESDataImportException e) {
            throw e;
        } catch (Exception e) {
            throw new ESDataImportException(e);
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
    @Override
    protected void loadCurrentStatus(){


        try {
            /**
             * 初始化数据检索起始状态信息
             */
            List<Status> statuses = SQLExecutor.queryListWithDBName(Status.class, statusDbname, selectAllSQL);
            if(statuses == null || statuses.size() == 0){
                return;
            }
            boolean fromFirst = importContext.isFromFirst();

            /**
             * 已经完成的任务
             */
            List<Status> completed = new ArrayList<Status>();
            /**
             * 已经过期的任务，修改状态为已完成
             */
            List<Status> olded = new ArrayList<Status>();
            for(Status status : statuses){
                status.setRealPath(status.getFilePath());
                //判断任务是否已经完成，如果完成，则对任务进行相应处理

                if(isComplete(status)){
                    completed.add(status);
                    fileListenerService.addCompletedFileTask(status.getFileId(),new FileReaderTask(status.getFileId()
                            ,status,fileImportContext.getFileImportConfig()));
                    logger.info("Ignore complete file {}",status.getFilePath());
                    continue;
                }

                FileConfig fileConfig = getFileConfig(status.getFilePath());
                if(fileConfig == null) {
//                    completed.add(status);
//                    fileListenerService.addCompletedFileTask(status.getFileId(),new FileReaderTask(status.getFileId()
//                            ,status));
                    logger.info("Ignore file {} which config is removed.",status.getFilePath());
                    continue;
                }
                File logFile = new File(status.getFilePath());
                if(fileConfig.isEnableInode()) {

                    if(!logFile.exists()){
                        File inodeFile = FileInodeHandler.getFileByInode( fileConfig,status.getFileId());
                        if(inodeFile != null){
                            status.setRealPath(FileInodeHandler.change(inodeFile.getCanonicalPath()));
                        }
                        else
                        {
                            continue;
                        }
                    }
                    else {
                        String inode = FileInodeHandler.linuxInode(logFile);
                        if (inode == null ) {
                            File inodeFile = FileInodeHandler.getFileByInode(fileConfig, status.getFileId());

                            if (inodeFile != null) {
                                logger.info("inodeFile:{},status.fileid:{}",inodeFile.getCanonicalPath(),status.getFileId());
                                status.setRealPath(FileInodeHandler.change(inodeFile.getCanonicalPath()));
                            }
                            else{
                                continue;
                            }
                        }
                        else if (!status.getFileId().equals(inode)) {
                            File inodeFile = FileInodeHandler.getFileByInode(fileConfig, status.getFileId());
                            if (inodeFile != null) {
                                logger.info("inode:{},status.fileid:{} 不相等，老path:{},新path:{}",inode,status.getFileId(),status.getFilePath(),inodeFile.getCanonicalPath());
                                status.setRealPath(FileInodeHandler.change(inodeFile.getCanonicalPath()));
                            }
                            else{
                                handleOldedTask(status);
                                logger.info("status.fileid:{} 对应的文件不存在，老path:{}，忽略本文件采集",status.getFileId(),status.getFilePath());
                                continue;
                            }
                        }
                    }
                }
                else {
                    if(!logFile.exists()){
                        continue;
                    }
                }
                /**
                if(isOlded(status,fileConfig)){
                    olded.add(status);
                    logger.info("Ignore old file {}",status.getFilePath());
                    continue;
                }
                if(isNeedClosed(status,fileConfig)){
                    fileListenerService.addOldedFileTask(status.getFileId(),new FileReaderTask(status.getFileId()
                            ,status));
                    handleOldedTask(status);
                    logger.info("Ignore need closed file {} closed old time {}",status.getFilePath(),fileConfig.getCloseOlderTime());
                    continue;
                }*/
                //需判断文件是否存在，不存在需清除记录
                //创建一个文件对应的交换通道
                FileResultSet kafkaResultSet = new FileResultSet(this.fileImportContext);
                FileTaskContext taskContext = new FileTaskContext(importContext,targetImportContext);
                final BaseDataTran fileDataTran = createBaseDataTran(taskContext,kafkaResultSet,  status);

                Thread tranThread = null;
                try {
                    if(fileDataTran != null) {
                        String tname = "file-log-tran|"+status.getRealPath();
                        if(fileConfig.isEnableInode()){
                            tname = tname + "|" +status.getFileId();
                        }
                        tranThread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                fileDataTran.tran();
                            }
                        }, tname);
                        tranThread.start();
                        if(logger.isInfoEnabled())
                            logger.info(tname+" started.");
                        Object lastValue = status.getLastValue();
                        long pointer = 0;
                        if (!fromFirst){
                            if (lastValue instanceof Long) {
                                pointer = (Long) lastValue;
                            } else if (lastValue instanceof Integer) {
                                pointer = ((Integer) lastValue).longValue();
                            } else if (lastValue instanceof Short) {
                                pointer = ((Short) lastValue).longValue();
                            }
                        }
                        else{
                            status.setLastValue(0l);
                        }
                        FileReaderTask task = new FileReaderTask(taskContext,new File(status.getRealPath())
                                ,status.getFileId()
                                ,fileConfig
                                ,pointer
                                ,fileListenerService,fileDataTran,status,fileImportContext.getFileImportConfig());
                        task.getFileInfo().setOriginFile(new File(status.getFilePath()));
                        task.getFileInfo().setOriginFilePath(status.getFilePath());
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
                        preCall(taskContext);//需要在任务完成时销毁taskContext
                        fileListenerService.addFileTask(task.getFileId(),task);
                        task.start();
                    }


                } catch (ESDataImportException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ESDataImportException(e);
                }
            }
            if(completed.size() > 0 && fileImportContext.getFileImportConfig().getRegistLiveTime() != null){
                handleCompletedTasks(completed ,false,fileImportContext.getFileImportConfig().getRegistLiveTime());
            }
            if(olded.size() > 0){
                handleOldedTasks(olded);
            }
        } catch (ESDataImportException e) {
            throw e;
        } catch (Exception e) {
            throw new ESDataImportException(e);
        }

    }
    private void stopScanThread(){
        if(logDirScanThreads != null){

            logger.info("StopScanThread:LogDirScanThread");
            for(LogDirScanThread logDirScanThread: logDirScanThreads){
                try {
                    logDirScanThread.stop();
                }
                catch (Exception e){

                }
            }
            logDirScanThreads = null;
        }
    }
    @Override
    public void destroy(boolean waitTranStop){
        this.status = TranConstant.PLUGIN_STOPAPPENDING;
        stopScanThread();
        fileListenerService.checkTranFinished();//检查所有的作业是否已经结束，并等待作业结束
        super.destroy( waitTranStop);//之前为什么是false super.destroy( false);
       // todo
    }

    protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet,Status currentStatus);
    @Override
    public void beforeInit() {
        if(importContext.getDbConfig() != null)
            this.initDS(importContext.getDbConfig());
    }
    protected void initFileListener(TaskContext taskContext){
        if(fileImportContext.getFileImportConfig() != null)
        {
            /**
            // 一个目录一个FileAlterationObserver 一个FileAlterationObserver一个线程进行监听扫描，有文件变化交给监听器进行处理
            //每个线程再开启5（可配置）个工作线程的线程池，用于多线程采集日志数据
            List<FileAlterationObserver> observerList = this.init(fileImportContext.getFileImportConfig());
            for(int i = 0; observerList != null && i < observerList.size(); i ++) {
                FileAlterationObserver fileAlterationObserver = observerList.get(i);
                FileAlterationMonitor fileAlterationMonitor = new FileAlterationMonitor(fileImportContext.getFileImportConfig().getInterval(),
                        fileAlterationObserver);
                this.fileAlterationMonitors.add(fileAlterationMonitor);
                try {
//                fileListener.reload();
//                    Iterator<FileAlterationObserver> iterator = fileAlterationMonitor.getObservers().iterator();
//                    while (iterator.hasNext()) {
//                        iterator.next().checkAndNotify();
//                    }
                    fileAlterationMonitor.start();
//                while(true){
//                    Thread.sleep(10000);
//                }
                } catch (Exception e) {
                    throw new ESDataImportException(e);
                }
            }*/
            List<FileConfig> fileConfigs = this.fileImportContext.getFileConfigList();
            if(fileConfigs != null && fileConfigs.size() > 0) {
                logDirScanThreads = new ArrayList<>(fileConfigs.size());
                for (FileConfig fileConfig : fileConfigs) {
                    if(fileConfig instanceof FtpConfig){
                        LogDirScanThread logDirScanThread = new FtpLogDirScanThread(fileImportContext.getFileImportConfig().getInterval(),
                                (FtpConfig)fileConfig, getFileListenerService());
                        logDirScanThreads.add(logDirScanThread);
                        logDirScanThread.start();
                    }
                    else {
                        LogDirScanThread logDirScanThread = new LogDirScanThread(fileImportContext.getFileImportConfig().getInterval(),
                                fileConfig, getFileListenerService());
                        logDirScanThreads.add(logDirScanThread);
                        logDirScanThread.start();
                    }
                }
            }


                synchronized (BackupSuccessFilesClean.class) {
                    if(backupSuccessFilesClean == null){
                        if (fileImportContext.getFileImportConfig() != null) {
                            if (fileImportContext.getFileImportConfig().isBackupSuccessFiles()) {
                                String backupSuccessFileDir = fileImportContext.getFileImportConfig().getBackupSuccessFileDir();
                                if (backupSuccessFileDir == null || backupSuccessFileDir.equals("")) {
                                    logger.warn("开启了备份成功文件机制，但是没有指定备份目录，忽略备份功能，请检查并设置backupSuccessFileDir");
                                } else {
                                    boolean backupEnable = fileImportContext.getFileImportConfig().getBackupSuccessFileInterval() > 0 && fileImportContext.getFileImportConfig().getBackupSuccessFileLiveTime() > 0;
                                    if (backupEnable) {
                                        backupSuccessFilesClean = new BackupSuccessFilesClean(fileImportContext.getFileImportConfig());
                                        backupSuccessFilesClean.start();
                                    }
                                }
                            }
                        }
                    }
                }



        }
    }


    @Override
    public void afterInit(){

    }
    @Override
    public void initStatusTableId() {

    }
    @Override
    public void importData() throws ESDataImportException {


        long importStartTime = System.currentTimeMillis();
        this.doImportData(null);
        long importEndTime = System.currentTimeMillis();
        if( isPrintTaskLog())
            logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());


    }
    @Override
    public void doImportData(TaskContext taskContext) throws ESDataImportException {

        initFileListener(taskContext);

    }
    @Override
    public void initSchedule(){
//        if(!fileImportContext.isFromFtp()) {
//            logger.info("Ignore initSchedule for plugin {}", this.getClass().getName());
//        }
//        else{
//            super.initSchedule();
//        }
    }
}
