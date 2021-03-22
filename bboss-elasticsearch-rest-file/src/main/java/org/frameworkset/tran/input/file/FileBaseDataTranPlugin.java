package org.frameworkset.tran.input.file;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.BaseDataTranPlugin;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/12
 */
public abstract class FileBaseDataTranPlugin extends BaseDataTranPlugin {
    protected FileImportContext fileImportContext;
    protected FileListener fileListener;
    protected List<FileAlterationObserver> observerList = new ArrayList<FileAlterationObserver>();
    protected FileAlterationMonitor fileAlterationMonitor;
    public FileBaseDataTranPlugin(ImportContext importContext,
                                  ImportContext targetImportContext,FileListener fileListener) {
        super(importContext, targetImportContext);
        this.fileImportContext = (FileImportContext) importContext;
        this.fileListener = fileListener;
    }

    protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet);
    @Override
    public void beforeInit() {

    }
    protected void initFileListener(TaskContext taskContext){
        if(fileImportContext.getFileImportConfig() != null)
        {
            this.init(fileImportContext.getFileImportConfig());
            fileAlterationMonitor =  new FileAlterationMonitor(fileImportContext.getFileImportConfig().getInterval(), observerList.toArray(new FileAlterationObserver[observerList.size()]));
            try {
                fileListener.reload();
                Iterator<FileAlterationObserver> iterator = fileAlterationMonitor.getObservers().iterator();
                while (iterator.hasNext()){
                    iterator.next().checkAndNotify();
                }
                fileAlterationMonitor.start();
                while(true){
                    Thread.sleep(10000);
                }
            } catch (Exception e) {
                e.printStackTrace();
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
    public void doImportData(TaskContext taskContext) throws ESDataImportException {

        initFileListener(taskContext);

    }
    public void initSchedule(){
        logger.info("Ignore initSchedule for plugin {}",this.getClass().getName());
    }
}
