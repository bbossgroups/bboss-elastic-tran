package org.frameworkset.tran.input.file;

import com.frameworkset.util.SimpleStringUtil;
import org.apache.commons.lang.StringUtils;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.record.CommonMapRecord;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xutengfei
 * @description
 * @create 2021/3/15
 */
public class FileReaderTask {
    private static Logger logger = LoggerFactory.getLogger(FileReaderTask.class);
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
    private BaseDataTran fileDataTran;
    private RandomAccessFile raf ;
    //状态 0启用 1失效
    public static final int STATUS_OK = 0;
    public static final int STATUS_NO = 1;
    private int status = STATUS_OK;
    public FileReaderTask(File file, String fileId, String fileHeadLineRegular, FileListenerService fileListenerService, BaseDataTran fileDataTran ) {
        this.file = file;
        this.pointer = 0;
        this.fileId = fileId;
        this.fileListenerService = fileListenerService;
        this.fileHeadLineRegular = fileHeadLineRegular;
        if(StringUtils.isNotEmpty(this.fileHeadLineRegular)){
            pattern = Pattern.compile(this.fileHeadLineRegular);
        }
        rootLevel = this.fileListenerService.getFileImportContext().getFileImportConfig().isRootLevel();
        this.fileDataTran = fileDataTran;

    }
    public FileReaderTask(File file,String fileId,String fileHeadLineRegular,long pointer,FileListenerService fileListenerService, BaseDataTran fileDataTran ) {
        this(file,fileId,fileHeadLineRegular,fileListenerService,fileDataTran);
        this.pointer = pointer;
    }
    public void start() {
        try {
            synchronized (file){
                if(raf == null) {
                    RandomAccessFile raf = new RandomAccessFile(file, "r");
                    raf.seek(pointer);
                    this.raf = raf;
                }
                //缓存
                StringBuilder builder = new StringBuilder();
                String line;
                List<Record> recordList = new ArrayList<>();
                while((line = raf.readLine())!=null){
                    line = new String(line.getBytes("ISO-8859-1"),"utf-8");
                    if(null != pattern){
                        Matcher m=pattern.matcher(line);
                        if(m.find() && builder.length()>0){
                            result(file,pointer,builder.toString(), recordList);
                            pointer = raf.getFilePointer();
//                            fileListenerService.flush();
                            builder = new StringBuilder();
                        }
                        if(builder.length() > 0){
                            builder.append(TranUtil.lineSeparator);
                        }
                        builder.append(line);
                    }else{
                        result(file,pointer,line, recordList);
                        pointer = raf.getFilePointer();
//                        fileListenerService.flush();
                    }
                }
                if(recordList.size() > 0 ){
                    fileDataTran.appendData(new CommonData(recordList));
                }

            }
        }catch (Exception e){
            logger.error("",e);
            throw new DataImportException("",e);
        }finally {
            destroy();
        }
    }

    public void destroy(){
        if(raf != null){
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            raf = null;
        }
    }

    private void result(File file, long pointer, String line,List<Record> recordList) {
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
        recordList.add(new CommonMapRecord(result,pointer));
//        System.out.println(SimpleStringUtil.object2json(result));
    }
    //公共数据
    private void common(File file, long pointer, Map result) {
        Map common = new HashMap();
        common.put("hostip","");
        common.put("hostname","");
        common.put("path",file.getAbsoluteFile());
        common.put("timestamp",new Date());
        common.put("pointer",pointer);
        common.put("fileId",fileId);
        result.put("@common",common);
    }

    public String getFileId() {
        return fileId;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "{\"file\":\""+ FileInodeHandler.change(file.getAbsolutePath())+"\",\"fileId\":\""+fileId+"\",\"pointer\":"+pointer+"}";
    }
}
