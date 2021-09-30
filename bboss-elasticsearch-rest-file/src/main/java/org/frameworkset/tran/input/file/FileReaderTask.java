package org.frameworkset.tran.input.file;

import com.frameworkset.util.BaseSimpleStringUtil;
import com.frameworkset.util.FileUtil;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Thread.sleep;

/**
 * @author xutengfei,yin-bp@163.com
 * @description
 * @create 2021/3/15
 */
public class FileReaderTask extends FieldManager{
    private static Logger logger = LoggerFactory.getLogger(FileReaderTask.class);
    private FileInfo fileInfo;
    private FileImportConfig fileImportConfig;


    /**
     * 文件监听事件服务
     */
    private FileListenerService fileListenerService;
    /**
     * 文件开始行标识正则
     */
    private Pattern pattern;
    private boolean rootLevel;
    private boolean enableMeta;

    private BaseDataTran fileDataTran;
    private RandomAccessFile raf ;
    //状态 0启用 1失效
    public static final int STATUS_OK = 0;
    public static final int STATUS_NO = 1;
    private int status = STATUS_OK;
    private Status currentStatus;
    private volatile boolean taskEnded;
    /**
     * jsondata：标识文本记录是json格式的数据，true 将值解析为json对象，false - 不解析，这样值将作为一个完整的message字段存放到上报数据中
     */
    private boolean jsondata ;
    private Thread worker ;
    private long oldLastModifyTime = -1;
    private long checkFileModifyInterval = 3000l;
    private long closeOlderTime ;
    private CloseOldedFileAssert closeOldedFileAssert;

    private IgnoreFileAssert ignoreFileAssert;
    private long ignoreOlderTime ;
    private TaskContext taskContext;
    private FileConfig fileConfig;
    /**
     * 文件采集偏移量
     */
    private long pointer;

    public FileReaderTask(TaskContext taskContext,File file, String fileId, FileConfig fileConfig,
                          FileListenerService fileListenerService,
                          BaseDataTran fileDataTran,
                          Status currentStatus ,FileImportConfig fileImportConfig ) {
        this.fileImportConfig = fileImportConfig;
        this.fileListenerService = fileListenerService;
        String charSet = fileConfig.getCharsetEncode() ;
        if(charSet == null || charSet.equals("")){
            charSet = this.fileListenerService.getFileImportContext()
                    .getFileImportConfig().getCharsetEncode();
        }
        this.pointer = 0;
        this.fileInfo = new FileInfo(charSet,
                                        FileInodeHandler.change(file.getAbsolutePath()),
                                    file,  fileId, fileConfig);
		fileInfo.setCloseEOF(fileConfig.isCloseEOF());
        this.fileConfig = fileConfig;
        this.taskContext = taskContext;

        if(fileConfig.getFileHeadLineRexPattern() != null){
            pattern = fileConfig.getFileHeadLineRexPattern() ;
        }
        closeOlderTime = fileConfig.getCloseOlderTime() == null?0:fileConfig.getCloseOlderTime();
        ignoreOlderTime = fileConfig.getIgnoreOlderTime() == null?0:fileConfig.getIgnoreOlderTime();
        this.closeOldedFileAssert = fileConfig.getCloseOldedFileAssert();
        this.ignoreFileAssert = fileConfig.getIgnoreFileAssert();
        rootLevel = this.fileListenerService.getFileImportContext().getFileImportConfig().isRootLevel();
        jsondata = this.fileListenerService.getFileImportContext().getFileImportConfig().isJsondata();
        enableMeta = this.fileListenerService.getFileImportContext().getFileImportConfig().isEnableMeta();
        checkFileModifyInterval = this.fileListenerService.getFileImportContext().getFileImportConfig().getCheckFileModifyInterval();
        this.fileDataTran = fileDataTran;
        this.currentStatus = currentStatus;


    }
    public FileReaderTask(String fileId,  Status currentStatus,FileImportConfig fileImportConfig ) {
        this.fileImportConfig = fileImportConfig;
        this.currentStatus = currentStatus;
        this.fileInfo = new FileInfo( fileId);
    }

    public FileInfo getFileInfo() {
        return fileInfo;
    }

    public TaskContext getTaskContext() {
        return taskContext;
    }

    /**
     * 检测文件是否被重命名，如果重命名则标识文件还存在
     * 1 文件被删除
     * 2 文件被重命名，能够识别本目录下面重命名的文件
     * -1 未知状态  判断过程中出错了，返回未知状态
     * @param logFileId
     * @return
     */
    public int fileExist(String logFileId){
        FileConfig fileConfig = fileInfo.getFileConfig();
        if(!fileInfo.getFileConfig().isEnableInode()){
            return 1;
        }
        File logDir = fileConfig.getLogDir();
        FilenameFilter filter = fileConfig.getFilter();
        try {
            if (logDir.isDirectory() && logDir.exists()) {
                File[] files = logDir.listFiles(filter);
                File file = null;
                for (int i = 0; files != null && i < files.length; i++) {
                    file = files[i];
                    String fileId = FileInodeHandler.inode(file,fileConfig.isEnableInode());
                    if(fileId.equals(logFileId) ){
                        //文件存在，被重命名了
                        return 2;
                    }
                }
            }
            //文件被删除了
            return 1;
        }
        catch(Exception e){
            //判断过程中出错了，返回未知状态
            return -1;
        }

    }

    /**
     * 判断文件是否被重命名
     * @param logFile
     * @return
     */
    public boolean fileRenamed(File logFile){
        FileConfig fileConfig = fileInfo.getFileConfig();
        if(!fileConfig.isEnableInode()){
            return false;
        }
        String logFileId = FileInodeHandler.inode(logFile,fileConfig.isEnableInode());
        return  !fileInfo.getFileId().equals(logFileId);

    }

    public FileReaderTask(TaskContext taskContext,File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
                          Status currentStatus ,FileImportConfig fileImportConfig   ) {
        this(  taskContext,file,fileId,  fileConfig,fileListenerService,fileDataTran,currentStatus,  fileImportConfig );
        this.pointer = pointer;
    }
    public void start(){
        String threadName = null;
        if(fileConfig.isEnableInode()) {
            threadName = "FileReaderTask-Thread|" + fileInfo.getFilePath() + "|" + fileInfo.getFileId();
        }
        else{
            threadName = "FileReaderTask-Thread|"+fileInfo.getFilePath() ;

        }
//        worker.setDaemon(true);
        worker = new Thread(new Work(),threadName );

        worker.start();
        if(logger.isInfoEnabled())
            logger.info(threadName+" started.");
    }
    public String getFilePath() {
        return fileInfo.getFilePath();
    }
    public boolean isEnableInode(){
        return fileInfo.getFileConfig().isEnableInode();
    }
    class Work implements Runnable{

        @Override
        public void run() {
            boolean delete = false;
            boolean olded = false;
            File file = fileInfo.getFile();
            String fileId = fileInfo.getFileId();
            do {
                if(taskEnded || fileListenerService.getBaseDataTranPlugin().checkTranToStop()){
                    break;
                }
                if(file.exists()){

                    long lastModifyTime = file.lastModified();
                    if(oldLastModifyTime == -1){
                        oldLastModifyTime = lastModifyTime;
                        execute();
                        continue;
                    }
                    else if(oldLastModifyTime == lastModifyTime){
                        long idleTime = System.currentTimeMillis() - oldLastModifyTime;
                        if(closeOlderTime > 0 && idleTime >= closeOlderTime){//已经超过指定的最大空闲静默时间，停止文件监控作业

                            if(closeOldedFileAssert == null) {

                                olded = true;
                            }
                            else{
                                olded = closeOldedFileAssert.canClose(fileInfo);
                                if(!olded) {
                                    if(logger.isDebugEnabled()) {
                                        logger.debug("文件[新：{}|{}，老：{}],idleTime:{},内容超过{}毫秒未变化，已经超过指定的最大空闲静默时间closeOlderTime，停止本文件采集作业,olded:{}",
                                                fileInfo.getFilePath(), fileInfo.getFileId(), fileInfo.getOriginFilePath(), idleTime, closeOlderTime, olded);
                                    }
                                }

                            }
                            if(olded) {
                                if(logger.isInfoEnabled())
                                    logger.info("文件[{}|{}]idleTime:{},内容超过{}毫秒未变化，已经超过指定的最大空闲静默时间closeOlderTime，停止本文件采集作业.",
                                        fileInfo.getFilePath(),fileInfo.getFileId(),idleTime,closeOlderTime);
                                break;
                            }
                        }
                        if(ignoreOlderTime > 0 && idleTime >= ignoreOlderTime){//已经超过指定的最大空闲静默时间，停止文件监控作业
//                            logger.info("file[{}|{}] idleTime:{},ignoreOlderTime:{}",fileInfo.getFilePath(),fileInfo.getFileId(),idleTime,ignoreOlderTime);


                            if(ignoreFileAssert == null) {

                                olded = true;
                            }
                            else{
                                olded = ignoreFileAssert.canIgnore(fileInfo);
                            }
                            if(olded) {
                                if(logger.isInfoEnabled())
                                    logger.info("文件[{}|{}]idleTime:{},内容超过{}毫秒未变化，已经超过指定的最大空闲静默时间ignoreOlderTime，停止本文件采集作业.",
                                        fileInfo.getFilePath(),fileInfo.getFileId(),idleTime,ignoreOlderTime);
                                break;
                            }
                        }
                        if(closeOldedFileAssert != null) {
                            olded = closeOldedFileAssert.canClose(fileInfo);
                            if(olded){
                                logger.info("备份日志文件[新：{}|{},老:{}]内容已经采集完毕，停止本文件采集作业.",
                                        fileInfo.getFilePath(),fileInfo.getFileId(),fileInfo.getOriginFilePath());
                                break;
                            }
                        }
                        if(ignoreFileAssert != null) {
                            olded = ignoreFileAssert.canIgnore(fileInfo);
                            if(olded){
                                logger.info("备份日志文件[新：{}|{},老:{}]内容已经采集完毕，停止本文件采集作业.",
                                        fileInfo.getFilePath(),fileInfo.getFileId(),fileInfo.getOriginFilePath());
                                break;
                            }
                        }
                        try {
                            sleep(checkFileModifyInterval);
                        } catch (InterruptedException e) {
                            break;
                        }
                        continue;
                    }
                    else{
                        if(fileRenamed(file)) //文件重命名，等待文件被清理重新更新新的File对象
                        {
                            File fileIdFile = FileInodeHandler.getFileByInode(fileConfig,fileId);//查找重命名后的文件
                            if (fileIdFile != null) {//文件发生了重命名
                                String filePath = FileInodeHandler.change(fileIdFile.getAbsolutePath());
                                if (logger.isInfoEnabled())
                                    logger.info("Rename Log file {} to {}", fileInfo.getOriginFilePath(), filePath);
                                changeFile(filePath,fileIdFile);
                                file = fileIdFile;
                                if(!fileInfo.isCloseEOF() && fileConfig.isCloseRenameEOF())
                                	fileInfo.setCloseEOF(true);//设置关闭标识，重命名的文件被采集完后关闭
                            }
                            else{
                                delete = true;
                                break;
                            }

                        }
                        oldLastModifyTime = lastModifyTime;
                        execute();
                        continue;
                    }
                }
                else{ //可能的删除文件，待处理
                    int lable = fileExist(fileId);//根据文件号识别文件是否被删除
                    if(lable == 1 || lable == -1)//文件被删除或未知
                    {

                        delete = true;
                        break;
                    }
                    try {
                        sleep(checkFileModifyInterval);
                    } catch (InterruptedException e) {
                        break;
                    }
                    continue;
                }



            }while(true);
            if(delete){
                //文件被删除，只清理作业任务，停止转换通道，清理和销毁通道任务上下文，当文件回来或者文件恢复后重新分配新的采集通道采集数据
                if(logger.isInfoEnabled())
                    logger.info("文件[{}]被删除，只清理作业任务，停止转换通道，清理和销毁通道任务上下文，当文件回来或者文件恢复后重新分配新的采集通道采集数据",fileInfo.getFilePath());
                fileListenerService.doDelete(fileId);
                //need test
                /**
                fileListenerService.addCompletedFileTask(currentStatus.getFileId(),new FileReaderTask(currentStatus.getFileId()
                        ,currentStatus));
                fileDataTran.getDataTranPlugin().handleOldedTask(currentStatus);
                 */
                taskEnded();
                fileDataTran.getDataTranPlugin().afterCall(getTaskContext());
                destroyTaskContext();
            }
            if(olded){
                taskEnded();
                fileListenerService.addOldedFileTask(currentStatus.getFileId(),new FileReaderTask(currentStatus.getFileId()
                        ,currentStatus,fileImportConfig));
                fileDataTran.getDataTranPlugin().handleOldedTask(currentStatus);
                fileDataTran.getDataTranPlugin().afterCall(getTaskContext());
                destroyTaskContext();
            }
        }

    }
    class Line{
        private String line;
        private boolean eof;
        private boolean eol;
        Line(String line, boolean eof,boolean eol) {
            this.line = line;
            this.eof = eof;
            this.eol = eol;
        }

        public String getLine() {
            return line;
        }



        public boolean isEof() {
            return eof;
        }

        public boolean isRollbackPreLine(){
//            return eof && !eol && !fileInfo.getFileConfig().isCloseEOF();
			return eof && !eol && !fileInfo.isCloseEOF();
        }


    }
    public Status getCurrentStatus() {
        return currentStatus;
    }
    /**
     * Reads the next line of text from this file.  This method successively
     * reads bytes from the file, starting at the current file pointer,
     * until it reaches a line terminator or the end
     * of the file.  Each byte is converted into a character by taking the
     * byte's value for the lower eight bits of the character and setting the
     * high eight bits of the character to zero.  This method does not,
     * therefore, support the full Unicode character set.
     *
     * <p> A line of text is terminated by a carriage-return character
     * ({@code '\u005Cr'}), a newline character ({@code '\u005Cn'}), a
     * carriage-return character immediately followed by a newline character,
     * or the end of the file.  Line-terminating characters are discarded and
     * are not included as part of the string returned.
     *
     * <p> This method blocks until a newline character is read, a carriage
     * return and the byte following it are read (to see if it is a newline),
     * the end of the file is reached, or an exception is thrown.
     *
     * @return     the next line of text from this file, or null if end
     *             of file is encountered before even one byte is read.
     * @exception  IOException  if an I/O error occurs.
     */

    public final Line readLine(long startPointer) throws IOException {
        StringBuilder input = new StringBuilder();
        int c = -1;
        boolean eol = false;

        while (!eol) {
            switch (c = raf.read()) {
                case -1:
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = raf.getFilePointer();
                    if ((raf.read()) != '\n') {
                        raf.seek(cur);
                    }
                    break;
                default:
                    input.append((char)c);
                    break;
            }
        }

        if ((c == -1) ) {
            if(input.length() == 0)
                return new Line(null,true,true);
            else{
//                if(fileConfig.isCloseEOF())
				if(fileInfo.isCloseEOF())
                    return new Line(input.toString(),true,false);
                else{ // 需要结束本次采集
                    raf.seek(startPointer);
                    return new Line(null,true,false);
                }
            }
        }
        else
            return new Line(input.toString(),false,eol);
    }

    private boolean reachEOFClosed(Line line){
//        if(fileConfig.isCloseEOF() && line.isEof()){
		if(fileInfo.isCloseEOF() && line.isEof()){
            return true;
        }
        return false;
    }
    private void execute() {
        boolean reachEOFClosed = false;
        File file = fileInfo.getFile();
        try {
            if(taskEnded)
                return;

               String charsetEncode = fileInfo.getCharsetEncode();
//            synchronized (this){  单线程处理，无需同步处理
                if(raf == null) {
                    RandomAccessFile raf = new RandomAccessFile(file, "r");
                    //文件重新写了，则需要重新读取
                    if(pointer > raf.length()){
                        pointer = 0;
                        this.currentStatus.setLastValue(0l);
                    }
                    raf.seek(pointer);
                    this.raf = raf;
                }
                //缓存
                StringBuilder builder = new StringBuilder();
                Line line_;
                String line = null;
                List<Record> recordList = new ArrayList<Record>();
                //批量处理记录数
                int fetchSize = this.fileListenerService.getFileImportContext().getFetchSize();

                long startPointer = pointer;
                while(true){
                    line_ = readLine( startPointer);
                    reachEOFClosed = reachEOFClosed(line_);
                    if(line_.getLine() != null) {
                        line = line_.getLine();
                        if (charsetEncode != null)
                            line = new String(line.getBytes("ISO-8859-1"), charsetEncode);

                        if (null != pattern) {
                            Matcher m = pattern.matcher(line);
                            if (m.find() && builder.length() > 0) {
                                pointer = raf.getFilePointer();
                                result(file, pointer, builder.toString(), recordList,reachEOFClosed);
                                startPointer =  pointer;
                                //分批处理数据
                                if (fetchSize > 0 && ( recordList.size() >= fetchSize)) {
                                    fileDataTran.appendData(new CommonData(recordList));
                                    recordList = new ArrayList<Record>();
                                }

                                builder.setLength(0);
                            }
                            if (builder.length() > 0) {
                                builder.append(TranUtil.lineSeparator);
                            }
                            builder.append(line);
                            if(reachEOFClosed){
                                pointer = raf.getFilePointer();
                                result(file,pointer,builder.toString(), recordList,reachEOFClosed);
                                startPointer =  pointer;

                                builder.setLength(0);
                                break;
                            }
                        } else {
                            pointer = raf.getFilePointer();
                            result(file, pointer, line, recordList,reachEOFClosed);
                            startPointer =  pointer;
                            //分批处理数据
                            if (fetchSize > 0 && recordList.size() >= fetchSize) {
                                fileDataTran.appendData(new CommonData(recordList));
                                recordList = new ArrayList<Record>();
                            }
                            if(reachEOFClosed)
                                break;

                        }
                    }
                    else{
                        break;
                    }
                }
                if(builder.length() > 0 ){
                    if(!line_.isRollbackPreLine()) {
                        pointer = raf.getFilePointer();
                        result(file, pointer, builder.toString(), recordList, reachEOFClosed);
                    }
                    builder.setLength(0);
                    builder = null;
                }
                if(recordList.size() > 0 ){
                    fileDataTran.appendData(new CommonData(recordList));
                }
                //如果设置了文件结束，及结束作业，则进行相应处理，需迁移到通道结束处进行归档和删除处理
                if(reachEOFClosed){
                    if(logger.isInfoEnabled())
                        logger.info("{} reached eof and will be closed.",toString());
                    /**
                     * 发送空记录
                     */
                    recordList = new ArrayList<Record>(1);
                    pointer = raf.getFilePointer();
                    recordList.add(new FileLogRecord(taskContext,true,pointer,reachEOFClosed));
                    fileDataTran.appendData(new CommonData(recordList));

                    fileListenerService.moveTaskToComplete(this);
                    this.taskEnded();

                }
//            }
        }catch (Exception e){
//            logger.error("",e);
            throw new DataImportException("",e);
        }finally {
            destroy();
            try {
                //需要删除采集完数据的eof文件，有必要进行优化并在回调函数中处理
                if (reachEOFClosed ) {
                    if (fileImportConfig.isBackupSuccessFiles())//备份采集完的数据文件，默认保留一周，过期清理
                        FileUtil.bakFile(file.getCanonicalPath(), SimpleStringUtil.getPath(fileImportConfig.getBackupSuccessFileDir(),file.getName()));
                    else if(fileConfig.isDeleteEOFFile())//删除日志文件
                        file.delete();

                }
            }
            catch (Exception e){
                logger.warn("",e);
            }
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

    public void destroyTaskContext(){
        taskContext = null;
    }

    /**
     * 检查记录内容是否是需要采集的记录内容
     * @param line
     * @return
     */
    private boolean regexCheck(String line,Pattern[] patterns,LineMatchType matchType){
//        Pattern[] includes = fileConfig.getIncludeLinesRexPattern();
//        Pattern[] excludes = fileConfig.getExcludeLinesRexPattern();
            boolean find = false;
            for(Pattern inc:patterns){
                find = matchType == LineMatchType.REGEX_CONTAIN?inc.matcher(line).find():
                                                                inc.matcher(line).matches();
                if(find)
                    break;
            }
            return find;

    }

    /**
     * 检查记录内容是否是需要采集的记录内容
     * @param line
     * @return
     */
    private boolean stringCheck(String line,String[] patterns,LineMatchType matchType){
//        Pattern[] includes = fileConfig.getIncludeLinesRexPattern();
//        Pattern[] excludes = fileConfig.getExcludeLinesRexPattern();
        boolean find = false;
        for(String inc:patterns){
            switch (matchType){
                case STRING_CONTAIN:
                    find = line.contains(inc);
                    break;
                case STRING_EQUALS:
                    find = line.equals(inc);
                    break;
                case STRING_END:
                    find = line.endsWith(inc);
                    break;
                case STRING_PREFIX:
                    find = line.startsWith(inc);
                    break;
                default:
                    break;
            }
            if(find)
                break;
        }
        return find;

    }

    private boolean includeCheck(String line,String[] includeLines,LineMatchType includeLineMatchType){
        boolean find = false;
        switch (includeLineMatchType){
            case REGEX_CONTAIN:
            case REGEX_MATCH:
                Pattern[] includes = fileConfig.getIncludeLinesRexPattern();
                find = regexCheck(line,includes,includeLineMatchType);
                break;
            case STRING_CONTAIN:
            case STRING_EQUALS:
            case STRING_END:
            case STRING_PREFIX:
                find = stringCheck(line,fileConfig.getIncludeLines(),includeLineMatchType);
                break;
            default:
                break;
        }
        return find;
    }

    private boolean excludeCheck(String line,String[] excludeLines,LineMatchType excludeLineMatchType){
        boolean find = false;
        if(excludeLines != null && excludeLines.length > 0){
            switch (excludeLineMatchType){
                case REGEX_CONTAIN:
                case REGEX_MATCH:
                    Pattern[] excludes = fileConfig.getExcludeLinesRexPattern();
                    if(regexCheck(line,excludes,excludeLineMatchType)){
                        find = true;
                    }
                    break;
                case STRING_CONTAIN:
                case STRING_EQUALS:
                case STRING_END:
                case STRING_PREFIX:
                    if(stringCheck(line,excludeLines,excludeLineMatchType))
                        find = true;
                    break;
                default:
                    break;
            }
        }
        return find;
    }
    /**
     * 检查记录内容是否是需要采集的记录内容
     * @param line
     * @return
     */
    private boolean check(String line){
        String[] includeLines = fileConfig.getIncludeLines();
        String[] excludeLines = fileConfig.getExcludeLines();
        LineMatchType includeLineMatchType = fileConfig.getIncludeLineMatchType();
        LineMatchType excludeLineMatchType = fileConfig.getExcludeLineMatchType();
        if(includeLines != null && includeLines.length > 0){
            boolean find = includeCheck(line,includeLines, includeLineMatchType);
            if(find){
                if(excludeCheck( line, excludeLines, excludeLineMatchType))
                    find = false;

            }
            return find;
        }
        else{
            boolean find = true;
            if(excludeCheck( line, excludeLines, excludeLineMatchType))
                find = false;
            return find;
        }

    }

    /**
     * 检查记录内容长度是否超过最大长度，如果超过就需要截取掉超过的内容
     * @param line
     * @return
     */
    private String checkMaxLength(String line){
        int maxLength = fileConfig.getMaxBytes();
        if(maxLength > 0 && line.length() > maxLength){
            line = line.substring(0,maxLength);
        }
        return line;
    }
    private void result(File file, long pointer, String line,List<Record> recordList,boolean reachEOFClosed) {
        if(!check(line)){
            recordList.add(new FileLogRecord(taskContext,true,pointer,reachEOFClosed));
        }
        else {

            Map result = new HashMap();
            try {
                if (jsondata) {
                    //json
                    Map json = SimpleStringUtil.json2Object(line, Map.class);
                    Map addFields = this.getAddFields();
                    if(addFields != null && addFields.size() > 0){
                        json.putAll(addFields);
                    }
                    Map<String, Object> ignoreFields = getIgnoreFields();
                    if(ignoreFields != null && ignoreFields.size() > 0){
                        Iterator iterator = ignoreFields.keySet().iterator();
                        while (iterator.hasNext()){
                            json.remove(iterator.next());
                        }
                    }
                    //同级
                    if (rootLevel) {

                        result = json;
                    } else {//不同级
                        result.put("json", json);
                    }
                } else {
                    line = checkMaxLength(line);
                    result.put("@message", line);
                    Map addFields = getAddFields();
                    if(addFields != null && addFields.size() > 0){
                        result.putAll(addFields);
                    }
                }

            } catch (Exception e) {
                // not json
                result.put("@message", line);
                Map addFields = getAddFields();
                if(addFields != null && addFields.size() > 0){
                    result.putAll(addFields);
                }
            }
            Map common = common(file, pointer, result);
            if (enableMeta) {
                result.put("@filemeta", common);
                result.put("@timestamp",new Date());
            }
            recordList.add(new FileLogRecord(taskContext,common,result,pointer,reachEOFClosed));
        }

    }

    //公共数据
    private Map common(File file, long pointer, Map result) {
        Map common = new HashMap();
        common.put("hostIp", BaseSimpleStringUtil.getIp());
        common.put("hostName",BaseSimpleStringUtil.getHostName());
        common.put("filePath",FileInodeHandler.change(file.getAbsolutePath()));

        common.put("pointer",pointer);
        common.put("fileId",fileInfo.getFileId());
        return common;
    }

    public String getFileId() {
        return fileInfo.getFileId();
    }

//    private ReentrantLock changeFileLock = new ReentrantLock();
    public void changeFile(String filePath,File file){
        try {
//            changeFileLock.lock();
            fileInfo.setFile(file);
            fileInfo.setFilePath(filePath);
            if (currentStatus != null) {
//            currentStatus.setFilePath(FileInodeHandler.change(file.getAbsolutePath()));
                currentStatus.setRealPath(filePath);//设置实际文件地址，原文件地址不变
            }
        }
        finally {
//            changeFileLock.unlock();
        }
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "{\"file\":\""+ FileInodeHandler.change(fileInfo.getFile().getAbsolutePath())+"\",\"fileId\":\""
                +fileInfo.getFileId()+"\",\"pointer\":"+pointer+"}";
    }

    public boolean isTaskEnded() {
        return taskEnded;
    }

    public void taskEnded() {
        if(taskEnded)
            return;
        synchronized (this) {
            if(taskEnded)
                return;
            this.taskEnded = true;
//            this.currentStatus.setStatus(ImportIncreamentConfig.STATUS_COMPLETE);
            this.fileDataTran.stopTranOnly();

        }
    }
    public BaseDataTran getFileDataTran() {
        return fileDataTran;
    }

}
