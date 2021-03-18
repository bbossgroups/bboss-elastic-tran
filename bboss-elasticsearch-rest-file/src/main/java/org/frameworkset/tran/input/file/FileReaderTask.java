package org.frameworkset.tran.input.file;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.RandomAccessFile;
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
    public FileReaderTask(File file,String fileId,String fileHeadLineRegular,FileListenerService fileListenerService) {
        this.file = file;
        this.pointer = 0;
        this.fileId = fileId;
        this.fileListenerService = fileListenerService;
        this.fileHeadLineRegular = fileHeadLineRegular;
        if(StringUtils.isNotEmpty(this.fileHeadLineRegular)){
            pattern = Pattern.compile(this.fileHeadLineRegular);
        }
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
                            System.out.println(file.getAbsoluteFile()+":"+builder.toString());
                            pointer = raf.getFilePointer();
                            fileListenerService.flush();
                            builder = new StringBuilder();
                        }
                        if(builder.length() > 0){
                            builder.append("\r\n");
                        }
                        builder.append(line);
                    }else{
                        System.out.println(file.getAbsoluteFile()+":"+line);
                        pointer = raf.getFilePointer();
                        fileListenerService.flush();
                    }
                }
                raf.close();
            }
        }catch (Exception e){
            System.err.println(e.getMessage());
        }
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
