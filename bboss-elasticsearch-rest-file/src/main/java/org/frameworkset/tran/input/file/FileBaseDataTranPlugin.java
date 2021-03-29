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

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author xutengfei,yinbp
 * @description
 * @create 2021/3/12
 */
public abstract class FileBaseDataTranPlugin extends BaseDataTranPlugin {
    protected FileImportContext fileImportContext;
    protected FileListener fileListener;
    protected List<FileAlterationObserver> observerList = new ArrayList<FileAlterationObserver>();
    protected FileAlterationMonitor fileAlterationMonitor;
    public FileBaseDataTranPlugin(ImportContext importContext,
                                  ImportContext targetImportContext) {
        super(importContext, targetImportContext);
        this.fileImportContext = (FileImportContext) importContext;

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
    private String getHeadLineReg(String filePath) {
        filePath = FileInodeHandler.change(filePath).toLowerCase();
        List<FileConfig> list = fileImportContext.getFileImportConfig().getFileConfigList();
        for(FileConfig config : list){
            String source = FileInodeHandler.change(config.getSourcePath()).toLowerCase();
            if(filePath.startsWith(source)){
                return config.getFileHeadLineRegular();
            }
        }
        return null;
    }
    public void initFileTask(Status status,File file){
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
                tranThread.setDaemon(true);
                tranThread.start();
                String fileId = FileInodeHandler.inode(file);
                FileReaderTask task = new FileReaderTask(file,fileId,getHeadLineReg(file.getAbsolutePath()),fileListenerService,fileDataTran,status);
//                fileConfigMap.put(fileId,task);
                fileListenerService.addFileTask(fileId,task);
                task.execute();
            }
        } catch (ESDataImportException e) {
            throw e;
        } catch (Exception e) {
            throw new ESDataImportException(e);
        }
        finally {
//			kafkaResultSet.reachEend();
//			try {
//				countDownLatch.await();
//			} catch (InterruptedException e) {
//				if(logger.isErrorEnabled())
//					logger.error("",e);
//			}
        }
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


            List<Status> completed = new ArrayList<Status>();
            for(Status status : statuses){
                //判断任务是否已经完成，如果完成，则对任务进行相应处理
                if(isComplete(status)){
                    completed.add(status);
                    continue;
                }
                String filePath = status.getFilePath();
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
                        tranThread.setDaemon(true);
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
                            status.setLastValue(0);
                        }
                        FileListenerService fileListenerService = fileListener.getFileListenerService();
                        FileReaderTask task = new FileReaderTask(new File(filePath)
                                ,status.getFileId()
                                ,getHeadLineReg(filePath)
                                ,pointer
                                ,fileListenerService,fileDataTran,status);
                        fileListenerService.addFileTask(task.getFileId(),task);
                    }


                } catch (ESDataImportException e) {
                    throw e;
                } catch (Exception e) {
                    throw new ESDataImportException(e);
                }


            }
            if(completed != null){
                handleCompletedTasks(completed ,false);
            }
        } catch (ESDataImportException e) {
            throw e;
        } catch (Exception e) {
            throw new ESDataImportException(e);
        }

    }


    protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet,Status currentStatus);
    @Override
    public void beforeInit() {

    }
    protected void initFileListener(TaskContext taskContext){
        if(fileImportContext.getFileImportConfig() != null)
        {
            this.init(fileImportContext.getFileImportConfig());
            fileAlterationMonitor =  new FileAlterationMonitor(fileImportContext.getFileImportConfig().getInterval(), observerList.toArray(new FileAlterationObserver[observerList.size()]));
            try {
//                fileListener.reload();
                Iterator<FileAlterationObserver> iterator = fileAlterationMonitor.getObservers().iterator();
                while (iterator.hasNext()){
                    iterator.next().checkAndNotify();
                }
                fileAlterationMonitor.start();
                while(true){
                    Thread.sleep(10000);
                }
            } catch (Exception e) {
                throw new ESDataImportException(e);
            }
        }
    }
    //初始化监听器
    protected void init(final FileImportConfig fileImportConfig){
        IOFileFilter dir = FileFilterUtils.and(FileFilterUtils.directoryFileFilter(),
                HiddenFileFilter.VISIBLE);
        for(final FileConfig fileConfig:fileImportConfig.getFileConfigList()){
            IOFileFilter filter = FileFilterUtils.and(
                    FileFilterUtils.fileFileFilter(),
                    FileFilterUtils.asFileFilter(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return Pattern.matches(fileConfig.getFileNameRegular(), name);
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
