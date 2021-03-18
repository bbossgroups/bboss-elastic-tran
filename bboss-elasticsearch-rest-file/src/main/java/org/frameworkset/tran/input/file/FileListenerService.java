package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.input.file.FileReaderTask;

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
    public FileListenerService(FileImportContext fileImportContext) {
        this.fileConfigMap = new HashMap<String, FileReaderTask>();
        this.fileImportContext = fileImportContext;
    }
    public void doChange(File file){
        String fileId = FileInodeHandler.inode(file);
        if(!fileConfigMap.containsKey(fileId)){
            FileReaderTask task = new FileReaderTask(file,fileId,getHeadLineReg(file.getAbsolutePath()),this);
            fileConfigMap.put(fileId,task);
            task.start();
        }else{
            FileReaderTask task = fileConfigMap.get(fileId);
            task.setFile(file);
            task.start();
        }
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
            String line;
            while ((line = reader.readLine())!=null){
                builder.append(line);
            }
            List<Map> list = SimpleStringUtil.json2ListObject(builder.toString(),Map.class);
            for(Map map : list){
                String filePath = map.get("file").toString();
                FileReaderTask task = new FileReaderTask(new File(filePath)
                        ,map.get("fileId").toString()
                        ,getHeadLineReg(filePath)
                        ,Long.valueOf(map.get("pointer").toString())
                        ,this);
                fileConfigMap.put(task.getFileId(),task);
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
