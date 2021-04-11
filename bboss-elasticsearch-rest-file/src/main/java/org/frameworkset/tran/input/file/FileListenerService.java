package org.frameworkset.tran.input.file;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.file.monitor.FileEntry;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
    public FileListenerService(FileImportContext fileImportContext,FileBaseDataTranPlugin baseDataTranPlugin) {
        this.fileConfigMap = new HashMap<String, FileReaderTask>();
        this.completedTasks = new HashMap<String, FileReaderTask>();
        this.oldedTasks = new HashMap<String, FileReaderTask>();
        this.fileImportContext = fileImportContext;
        this.baseDataTranPlugin = baseDataTranPlugin;
    }

    public void moveTaskToComplete(FileReaderTask fileReaderTask){
        synchronized(fileConfigMap) {
            fileConfigMap.remove(fileReaderTask.getFileId());
        }
        this.completedTasks.put(fileReaderTask.getFileId(), fileReaderTask);
    }

    public void doChange(File file){
        String fileId = FileInodeHandler.inode(file);
        if(completedTasks.containsKey(fileId)){ // 已经采集过的文件直接返回
            logger.info("file {} has complate collected." ,file.getAbsolutePath());
            return;
        }
        if(oldedTasks.containsKey(fileId)){ // 已经采集过的文件直接返回
            logger.info("file {} olded collected." ,file.getAbsolutePath());
            return;
        }
        if(!fileConfigMap.containsKey(fileId) ){
            FileConfig fileConfig = baseDataTranPlugin.getFileConfig(file.getAbsolutePath());
            if(fileConfig == null)
                return;
            Status currentStatus = new Status();
            currentStatus.setId(fileId.hashCode());
            currentStatus.setTime(new Date().getTime());
            currentStatus.setFileId(fileId);
            currentStatus.setFilePath(FileInodeHandler.change(file.getAbsolutePath()));
            currentStatus.setStatus(ImportIncreamentConfig.STATUS_COLLECTING);
            long pointer = fileConfig.getStartPointer() !=null && fileConfig.getStartPointer() > 0l ?fileConfig.getStartPointer():0l;
            currentStatus.setLastValue(pointer);

            boolean successed = baseDataTranPlugin.initFileTask(fileConfig,currentStatus,file,pointer);



        }else{

            FileReaderTask task = fileConfigMap.get(fileId);
            task.setFile(file);
            task.dataChange();
        }
    }

    public void addCompletedFileTask(String fileId,FileReaderTask fileReaderTask){

        synchronized(completedTasks) {
            completedTasks.put(fileId,fileReaderTask);
        }
    }

    public void addOldedFileTask(String fileId,FileReaderTask fileReaderTask){

        synchronized(oldedTasks) {
            oldedTasks.put(fileId,fileReaderTask);
        }
    }
    public void addFileTask(String fileId,FileReaderTask fileReaderTask){

        synchronized(fileConfigMap) {
            fileConfigMap.put(fileId,fileReaderTask);
        }
    }
    //文件删除linux环境 文件删除了确实是文件删除了
    //window 环境无法判断 直接remove调
    public  void doDelete(FileEntry entry) {
        FileReaderTask fileReaderTask = null;
        synchronized(fileConfigMap) {
            fileReaderTask = fileConfigMap.remove(entry.getFileId());
        }
        if(fileReaderTask != null){
            this.completedTasks.put(entry.getFileId(), fileReaderTask);
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
                }
            });
            thread.start();
            // todo 删除文件状态更新
        }

    }
    //文件移动 linux环境才能根据inode判断文件移动了
    public void onFileMove(File oldFile, File newFile) {
        String fileId = FileInodeHandler.inode(newFile);
        FileReaderTask fileReaderTask = null;
        synchronized(fileConfigMap) {
            fileReaderTask = fileConfigMap.get(fileId);
        }
        //不存在的不处理，会有创建事件去处理了
        if(fileReaderTask != null){
            fileReaderTask.changeFile(newFile);
            //文件移到不需要执行采集操作
//            fileReaderTask.execute();
        }
    }
    public FileImportContext getFileImportContext() {
        return fileImportContext;
    }

    public void checkTranFinished() {
        synchronized(fileConfigMap) {
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

    }
/**
    private String getHeadLineReg(String filePath) {
        filePath = FileInodeHandler.change(filePath).toLowerCase();
        List<FileConfig> list = fileImportContext.getFileImportConfig().getFileConfigList();
        for(FileConfig config : list){
            String source = config.getNormalSourcePath();
            if(filePath.startsWith(source)){
                return config.getFileHeadLineRegular();
            }
        }
        return null;
    }

    public void flush(){
        synchronized (this){
            FileWriter writer = null;
            try{
                StringBuilder builder = new StringBuilder("[");
                for(String key : fileConfigMap.keySet()){
                    builder.append(fileConfigMap.get(key)).append(",");
                }
                builder = builder.replace(builder.length()-1,builder.length(),"]");
                File file = new File(System.getProperty("user.dir")+File.separator+"data.json");
                writer = new FileWriter(file);
                writer.write(builder.toString());
                writer.flush();
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                if(writer != null){
                    try {
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public void reload(){
        File file = new File(System.getProperty("user.dir")+File.separator+"data.json");
        FileReader fileReader = null;
        BufferedReader reader = null;
        try {
            if(!file.exists()){
                return;
            }
            fileReader = new FileReader(file);
            reader = new BufferedReader(fileReader);
            StringBuilder builder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine())!=null){
                builder.append(line);
            }
            List<Map> list = SimpleStringUtil.json2ListObject(builder.toString(),Map.class);
            for(Map map : list){
                String filePath = map.get("file").toString();
                //需判断文件是否存在，不存在需清除记录
                //创建一个文件对应的交换通道
                FileResultSet kafkaResultSet = new FileResultSet(this.fileImportContext);
//		final CountDownLatch countDownLatch = new CountDownLatch(1);
                final BaseDataTran fileDataTran = ((FileBaseDataTranPlugin)baseDataTranPlugin).createBaseDataTran((TaskContext)null,kafkaResultSet);

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
                        FileReaderTask task = new FileReaderTask(new File(filePath)
                                ,map.get("fileId").toString()
                                ,getHeadLineReg(filePath)
                                ,Long.valueOf(map.get("pointer").toString())
                                ,this,fileDataTran);
                        fileConfigMap.put(task.getFileId(),task);
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
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try{
                if(reader != null){
                    reader.close();
                }
                if(fileReader != null){
                    fileReader.close();
                }
            }catch (Exception e){

            }
        }
    }
     */
}
