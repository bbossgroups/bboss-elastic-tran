package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.BaseDataTranPlugin;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.schedule.TaskContext;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/15
 */
public class FileListenerService {
    private Map<String, FileReaderTask> fileConfigMap;
    private FileImportContext fileImportContext;
    private BaseDataTranPlugin baseDataTranPlugin;
    public FileListenerService(FileImportContext fileImportContext,BaseDataTranPlugin baseDataTranPlugin) {
        this.fileConfigMap = new HashMap<String, FileReaderTask>();
        this.fileImportContext = fileImportContext;
        this.baseDataTranPlugin = baseDataTranPlugin;
    }
    public void doChange(File file){
        String fileId = FileInodeHandler.inode(file);
        if(!fileConfigMap.containsKey(fileId)){
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
                    FileReaderTask task = new FileReaderTask(file,fileId,getHeadLineReg(file.getAbsolutePath()),this,fileDataTran);
                    fileConfigMap.put(fileId,task);
                    task.start();
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

        }else{
            FileReaderTask task = fileConfigMap.get(fileId);
            task.setFile(file);
            task.start();
        }
    }

    public FileImportContext getFileImportContext() {
        return fileImportContext;
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
}
