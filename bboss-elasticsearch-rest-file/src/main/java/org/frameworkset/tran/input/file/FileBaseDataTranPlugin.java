package org.frameworkset.tran.input.file;

import com.frameworkset.common.poolman.SQLExecutor;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.BaseDataTranPlugin;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.file.monitor.FileAlterationMonitor;
import org.frameworkset.tran.file.monitor.FileAlterationObserver;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranConstant;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/12
 */
public abstract class FileBaseDataTranPlugin extends BaseDataTranPlugin {
    protected FileImportContext fileImportContext;
    protected FileListener fileListener;
//    protected List<FileAlterationObserver> observerList = new ArrayList<FileAlterationObserver>();
    protected List<FileAlterationMonitor> fileAlterationMonitors;
    public FileBaseDataTranPlugin(ImportContext importContext,
                                  ImportContext targetImportContext) {
        super(importContext, targetImportContext);
        this.fileImportContext = (FileImportContext) importContext;

    }
    public boolean isMultiTran(){
        return true;
    }
    public void setFileListener(FileListener fileListener) {
        this.fileListener = fileListener;
    }

    @Override
    public void initLastValueClumnName(){

    }
    public Status getCurrentStatus(){
        throw new UnsupportedOperationException("getCurrentStatus");
    }
    public FileConfig getFileConfig(String filePath) {
        filePath = FileInodeHandler.change(filePath).toLowerCase();
        List<FileConfig> list = fileImportContext.getFileConfigList();
        for(FileConfig config : list){
            Pattern source = config.getNormalSourcePathPattern();
            if(source.matcher(filePath).matches()){
                return config;
            }
        }
        return null;
    }

    public boolean initFileTask(FileConfig fileConfig,Status status,File file,long pointer){

        if(fileConfig == null){
            return false;
        }
        addStatus( status);
        FileResultSet kafkaResultSet = new FileResultSet(this.fileImportContext);
//		final CountDownLatch countDownLatch = new CountDownLatch(1);
        final BaseDataTran fileDataTran = createBaseDataTran((TaskContext)null,kafkaResultSet,status);
        FileListenerService fileListenerService = fileListener.getFileListenerService();
        Thread tranThread = null;
        try {
            if(fileDataTran != null) {
                tranThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        fileDataTran.tran();
                    }
                }, "file-log-tran");
                tranThread.start();
                String fileId = FileInodeHandler.inode(file);
                FileReaderTask task = new FileReaderTask(file,fileId,fileConfig,pointer,
                        fileListenerService,fileDataTran,status,fileImportContext.getFileTaskWorkQueue());
//                fileConfigMap.put(fileId,task);
                fileListenerService.addFileTask(fileId,task);
                task.dataChange();
            }
            return true;
        } catch (ESDataImportException e) {
            throw e;
        } catch (Exception e) {
            throw new ESDataImportException(e);
        }

    }
    private boolean isOlded(Status status,FileConfig fileConfig){
        if(fileConfig.getIgnoreOlderTime() == null)
            return false;
        long lastTime = status.getTime();
        long oldedTime = fileConfig.getIgnoreOlderTime();
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

            FileListenerService fileListenerService = fileListener.getFileListenerService();
            /**
             * 已经完成的任务
             */
            List<Status> completed = new ArrayList<Status>();
            /**
             * 已经过期的任务，修改状态为已完成
             */
            List<Status> olded = new ArrayList<Status>();
            for(Status status : statuses){
                //判断任务是否已经完成，如果完成，则对任务进行相应处理
                if(isComplete(status)){
                    completed.add(status);
                    fileListenerService.addCompletedFileTask(status.getFileId(),new FileReaderTask(status.getFileId()
                            ,status));
                    continue;
                }

                String filePath = status.getFilePath();
                FileConfig fileConfig = getFileConfig(filePath);
                if(fileConfig == null) {
                    completed.add(status);
                    fileListenerService.addCompletedFileTask(status.getFileId(),new FileReaderTask(status.getFileId()
                            ,status));
                    continue;
                }
                if(isOlded(status,fileConfig)){
                    olded.add(status);
                    fileListenerService.addOldedFileTask(status.getFileId(),new FileReaderTask(status.getFileId()
                            ,status));
                    continue;
                }
                //需判断文件是否存在，不存在需清除记录
                //创建一个文件对应的交换通道
                FileResultSet kafkaResultSet = new FileResultSet(this.fileImportContext);
                final BaseDataTran fileDataTran = createBaseDataTran((TaskContext)null,kafkaResultSet,  status);

                Thread tranThread = null;
                try {
                    if(fileDataTran != null) {
                        tranThread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                fileDataTran.tran();
                            }
                        }, "file-log-tran");
                        tranThread.start();
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

                        FileReaderTask task = new FileReaderTask(new File(filePath)
                                ,status.getFileId()
                                ,fileConfig
                                ,pointer
                                ,fileListenerService,fileDataTran,status,fileImportContext.getFileTaskWorkQueue());
                        fileListenerService.addFileTask(task.getFileId(),task);
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
    @Override
    public void destroy(boolean waitTranStop){

        for(int i = 0; fileAlterationMonitors != null && i < fileAlterationMonitors.size(); i ++) {
            FileAlterationMonitor fileAlterationMonitor = fileAlterationMonitors.get(i);
            try {
                fileAlterationMonitor.stop();
            } catch (Exception e) {
                logger.warn("fileAlterationMonitor stop error:", e);
            }
        }
        this.status = TranConstant.PLUGIN_STOPAPPENDING;
        fileListener.checkTranFinished();//检查所有的作业是否已经结束，并等待作业结束
        super.destroy( false);
       // todo
    }

    protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet,Status currentStatus);
    @Override
    public void beforeInit() {

    }
    protected void initFileListener(TaskContext taskContext){
        if(fileImportContext.getFileImportConfig() != null)
        {
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
            }
        }
    }
    //初始化监听器
    protected List<FileAlterationObserver> init(final FileImportConfig fileImportConfig){
        List<FileAlterationObserver> observerList = new ArrayList<FileAlterationObserver>();
        IOFileFilter dir = FileFilterUtils.and(FileFilterUtils.directoryFileFilter(),
                HiddenFileFilter.VISIBLE);
        for(final FileConfig fileConfig:fileImportConfig.getFileConfigList()){
            IOFileFilter filter = FileFilterUtils.and(
                    FileFilterUtils.fileFileFilter(),
                    FileFilterUtils.asFileFilter(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            Matcher m = fileConfig.getFileNameRexPattern().matcher(name);
                            return m.matches();
//                            return Pattern.matches(fileConfig.getFileNameRegular(), name);
                        }
                    })
            );
            if(fileConfig.isScanChild()){
                filter = FileFilterUtils.or(filter,dir);
            }
            // 装配过滤器
            FileAlterationObserver observer = new FileAlterationObserver(fileConfig.getSourcePath(), filter);
            // 向监听者添加监听器，并注入业务服务
            observer.addListener(fileListener);
            observerList.add(observer);
        }
        return observerList;
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
    public void initSchedule(){
        logger.info("Ignore initSchedule for plugin {}",this.getClass().getName());
    }
}
