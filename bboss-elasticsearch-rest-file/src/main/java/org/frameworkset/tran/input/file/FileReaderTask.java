package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/15
 */
public class FileReaderTask {
    /**
     * 文件
     */
    private File file;
    /**
     * 文件号
     */
    private String fileId;
    /**
     * 文件采集偏移量
     */
    private long pointer;
    /**
     * 文件开始行标识，正则表达式
     */
    private String fileHeadLineRegular;
    /**
     * 文件监听事件服务
     */
    private FileListenerService fileListenerService;
    /**
     * 文件开始行标识正则
     */
    private Pattern pattern;
    private boolean rootLevel;
    public FileReaderTask(File file,String fileId,String fileHeadLineRegular,FileListenerService fileListenerService) {
        this.file = file;
        this.pointer = 0;
        this.fileId = fileId;
        this.fileListenerService = fileListenerService;
        this.fileHeadLineRegular = fileHeadLineRegular;
        if(StringUtils.isNotEmpty(this.fileHeadLineRegular)){
            pattern = Pattern.compile(this.fileHeadLineRegular);
        }
        rootLevel = this.fileListenerService.getFileImportContext().getFileImportConfig().isRootLevel();
    }
    public FileReaderTask(File file,String fileId,String fileHeadLineRegular,long pointer,FileListenerService fileListenerService) {
        this(file,fileId,fileHeadLineRegular,fileListenerService);
        this.pointer = pointer;
    }
    public void start() {
        try {
            synchronized (file){
                RandomAccessFile raf = new RandomAccessFile(file,"r");
                raf.seek(pointer);
                //缓存
                StringBuilder builder = new StringBuilder();
                String line;
                while((line = raf.readLine())!=null){
                    line = new String(line.getBytes("ISO-8859-1"),"utf-8");
                    if(null != pattern){
                        Matcher m=pattern.matcher(line);
                        if(m.find() && builder.length()>0){
                            result(file,pointer,builder.toString());
                            pointer = raf.getFilePointer();
                            fileListenerService.flush();
                            builder = new StringBuilder();
                        }
                        if(builder.length() > 0){
                            builder.append("\r\n");
                        }
                        builder.append(line);
                    }else{
                        result(file,pointer,line);
                        pointer = raf.getFilePointer();
                        fileListenerService.flush();
                    }
                }
                raf.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void result(File file, long pointer, String line) {
        Map result = new HashMap();
        try{
            //json
            Map json = SimpleStringUtil.json2Object(line, Map.class);
            //同级
            if(rootLevel){
                result = json;
            }else{//不同级
                result.put("json",json);
            }
            common(file,pointer,result);
        }catch (Exception e){
            // not json
            common(file,pointer,result);
            result.put("message",line);
        }
        System.out.println(SimpleStringUtil.object2json(result));
    }
    //公共数据
    private void common(File file, long pointer, Map result) {
        Map common = new HashMap();
        common.put("hostip","");
        common.put("hostname","");
        common.put("path",file.getAbsoluteFile());
        common.put("timestamp",new Date());
        common.put("pointer",pointer);
        result.put("@common",common);
    }

    public String getFileId() {
        return fileId;
    }

    public void setFile(File file) {
        this.file = file;
    }
    @Override
    public String toString() {
        return "{\"file\":\""+FileInodeHandler.change(file.getAbsolutePath())+"\",\"fileId\":\""+fileId+"\",\"pointer\":"+pointer+"}";
    }
}
