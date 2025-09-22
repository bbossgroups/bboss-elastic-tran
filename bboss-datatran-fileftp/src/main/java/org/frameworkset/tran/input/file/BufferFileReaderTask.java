package org.frameworkset.tran.input.file;
/**
 * Copyright 2024 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.frameworkset.tran.record.CommonData;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import static org.frameworkset.tran.plugin.file.input.FileInputConfig.DEFAULT_BUFFER_CAPACITY;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/6/25
 */
public class BufferFileReaderTask extends FileReaderTask{
    private static Logger logger = LoggerFactory.getLogger(BufferFileReaderTask.class);
    private byte[] buffer;
    private int bufferCapacity = DEFAULT_BUFFER_CAPACITY;
    public BufferFileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, FileListenerService fileListenerService, BaseDataTran fileDataTran, Status currentStatus, FileInputConfig fileImportConfig) {
        super(taskContext, file, fileId, fileConfig, fileListenerService, fileDataTran, currentStatus, fileImportConfig);
        bufferCapacity = fileImportConfig.getBufferCapacity();
        buffer = new byte[bufferCapacity];
    }

    public BufferFileReaderTask(String fileId, Status currentStatus, FileInputConfig fileImportConfig) {
        super(fileId, currentStatus, fileImportConfig);
        bufferCapacity = fileImportConfig.getBufferCapacity();
        buffer = new byte[bufferCapacity];
    }

    public BufferFileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran, Status currentStatus, FileInputConfig fileImportConfig) {
        super(taskContext, file, fileId, fileConfig, pointer, fileListenerService, fileDataTran, currentStatus, fileImportConfig);
        bufferCapacity = fileImportConfig.getBufferCapacity();
        buffer = new byte[bufferCapacity];
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
     * 
     * @exception IOException  if an I/O error occurs.
     */
//    @Override
    public void readLines(ReadMetric readMetric, FileReadFunction lineHandler) throws Exception {
        long startPointer = readMetric.getStartPointer();
        long prePointer = raf.getFilePointer();
        int limit = raf.read(buffer);        
        long offset = 0l;
        int position = 0;
        //文件结束标记
        boolean eof = limit <= 0;
        while (!eof) {            
            byte c = -1;
            //行结尾
            boolean eol = false;            
            byte[] arrays = new byte[1024];
            int arrPos = 0;
            //循环消费缓冲区的数据
            while (!eol) {
                //没达到行尾，继续读取下一波缓存                
                if(position >= limit){
                    limit = raf.read(buffer);
                    eof = limit <= 0;
                    position = 0;
                    //如果没有读取到新数据
                    if(eof){
                        c = -1;
                        eol = true;
                        break;
                    }
                }
                switch (c = buffer[position]) {
                    case 10: //Unix or Linux line separator:
                        eol = true;                         
                        break;
                    case 13: //Windows or Mac line separator:
                        eol = true;
                        if (position < limit - 1) {
                            byte b1 = buffer[position + 1];
                            if (b1 == 10) {// Mac line separator{
                                position++;
                                offset ++;
                            }
                        }
                        break;
                    default:
                        if (arrPos >= arrays.length)
                            arrays = grow(arrays);
                        arrays[arrPos++] = c;
                        break;
                }
                position++;
                offset ++;
            }

            boolean continueRead = false;
            if (c == -1) {
                if (arrPos == 0) {
                    continueRead = lineHandler.apply(new Line(null, true, true,prePointer,startPointer+offset));
                }
                else {
                    if (fileInfo.isCloseEOF()) {
                        continueRead = lineHandler.apply(new Line(decode(arrays, arrPos), true, false,prePointer,startPointer+offset));
                    }
                    else { // 需要结束本次采集,并退回到采集开始为止
                        long rollbackPointer = readMetric.getStartPointer();
                        raf.seek(rollbackPointer);
                        continueRead = lineHandler.apply(new Line(null, true, false,rollbackPointer,rollbackPointer));
                    }
                }
            } else {
                long pos = startPointer+offset;
                continueRead = lineHandler.apply(new Line(decode(arrays, arrPos), false, eol,prePointer,pos));
                prePointer = pos;
            }
            if(!continueRead)
                break;            
        }
    }

    protected void execute() {
        File file = fileInfo.getFile();
        DataTranPlugin dataTranPlugin = fileListenerService.getBaseDataTranPlugin();
        InputPlugin inputPlugin = dataTranPlugin.getInputPlugin();
        try {
            if(taskEnded || inputPlugin.isStopCollectData())
                return;

            if(raf == null) {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                //文件重新写了，则需要重新读取
                if(pointer > raf.length()){
                    pointer = 0;
                    this.currentStatus.getCurrentLastValueWrapper().setLastValue(0L);
                }
                raf.seek(pointer);
                this.raf = raf;
            }
          
            List<Record> recordList = new ArrayList<Record>();
            //批量处理记录数
            int fetchSize = this.fileListenerService.getImportContext().getFetchSize();
            int skipHeaderLines = this.fileConfig.getSkipHeaderLines();
            long startPointer = pointer;
            
            boolean firstReader = startPointer == 0;
            
            
            ReadMetric readMetric = new ReadMetric();
            readMetric.setStartPointer(pointer);
            readMetric.setEnableMultiLine(pattern != null);
            readMetric.setRecordList(recordList);
            //缓存
            StringBuilder tmp = null;
            if(readMetric.isEnableMultiLine()){
                tmp = new StringBuilder();
            }
            final StringBuilder builder = tmp;
            readLines(readMetric, new FileReadFunction() {
                @Override
                public boolean apply(Line line_) throws Exception{
                    
                    readMetric.setReachEOFClosed(reachEOFClosed(line_));
                    readMetric.setLine(line_);
                    if(line_.getLine() != null) {
                        if(!readMetric.isCollectStarted()){
                            readMetric.setCollectStarted(true);
                        }
                        String line = line_.getLine();
                        if(firstReader && skipHeaderLines > 0 && readMetric.getReadLines() < skipHeaderLines){
                            logger.info("Skip line {}",readMetric.getReadLines());
                            pointer = line_.getOffset();
                            readMetric.setStartPointer(pointer);
                            readMetric.increamentReadLines();
                            if(!readMetric.isReachEOFClosed()) {
                                if(inputPlugin.isStopCollectData())
                                    return false;
                                return true;
                            }
                            else
                            {
                                return true;
                            }
                        }

                        if (readMetric.isEnableMultiLine()) {//多行记录匹配模式
                            Matcher m = pattern.matcher(line);
                            if (m.find() ) {//下行记录行开始
                                if (!readMetric.isMultiFirstRow()) {
                                    readMetric.setMultiFirstRow(true);

                                }
                                readMetric.setMultilineBegin(true);
                                if(builder.length() > 0) {
                                    pointer = line_.getOffset();
                                    //应该使用下行记录开始的位置

                                    result(file, line_.getPrePointer(), builder.toString(), readMetric.getRecordList(), readMetric.isReachEOFClosed());
                                    readMetric.setStartPointer(line_.getPrePointer());
                                    //分批处理数据
                                    if (fetchSize > 0 && (readMetric.recordSize() >= fetchSize)) {
                                        fileDataTran.appendData(new CommonData(readMetric.getRecordList()));
                                        try {
                                            fetchAwaitSleep();
                                        } catch (InterruptedException e) {
                                            return false;
                                        }
                                        readMetric.setRecordList(new ArrayList<Record>());
                                    }

                                    builder.setLength(0);
                                    if (inputPlugin.isStopCollectData()) {
                                        return false;
                                    }
                                }
                            }
                            else{                                
                                readMetric.setMultiFirstRow(false);
                            }
                           

                            if (builder.length() > 0) {
                                builder.append(TranUtil.lineSeparator);
                            }
                            builder.append(line);
                            if(readMetric.isReachEOFClosed()){
                                pointer = line_.getOffset();
                                result(file,pointer,builder.toString(), readMetric.getRecordList(),readMetric.isReachEOFClosed());

                                builder.setLength(0);
                                return false;
                            }
                        } else {
                            pointer = line_.getOffset();
                            result(file, pointer, line, readMetric.getRecordList(),readMetric.isReachEOFClosed());
                            readMetric.setStartPointer(pointer);
                            //分批处理数据
                            if (fetchSize > 0 && readMetric.recordSize() >= fetchSize) {
                                fileDataTran.appendData(new CommonData(readMetric.getRecordList()));
                                try {
                                    fetchAwaitSleep();
                                } catch (InterruptedException e) {
                                    return false;
                                }
                                readMetric.setRecordList(new ArrayList<Record>());
                            }
                            if(readMetric.isReachEOFClosed() || inputPlugin.isStopCollectData())
                                return false;

                        }
                    }
                    else{//空行处理
                        if(readMetric.getLine().isEol() && !readMetric.getLine().isEof()){
                            if(!readMetric.isEnableMultiLine()  //单行记录模式 
                                    || !readMetric.isCollectStarted())  //头部空行记录全部忽略
                                pointer = line_.getOffset();
                            return true;
                        }
                        else {
                            return false;
                        }
                    }
                    return true;
                }
            });

            /**
             * 多行记录未结束情况处理
             */
            
            if(readMetric.isEnableMultiLine() && builder.length() > 0 ){
                if(!readMetric.getLine().isRollbackPreLine(fileInfo)) {
                    pointer = readMetric.getLine().getOffset();
                    result(file, pointer, builder.toString(), readMetric.getRecordList(), readMetric.isReachEOFClosed());
                }
                else{
                    pointer = readMetric.getLine().getOffset();
                }
                builder.setLength(0);
            }
            if(readMetric.recordSize() > 0 ){
                fileDataTran.appendData(new CommonData(readMetric.getRecordList()));
                try {
                    fetchAwaitSleep();
                } catch (InterruptedException e) {

                }
            }
            //如果设置了文件结束，及结束作业，则进行相应处理，需迁移到通道结束处进行归档和删除处理
            if(readMetric.isReachEOFClosed()){

                /**
                 * 发送空记录
                 */

                pointer = readMetric.getLine().getOffset();
                sendReadEOFcloseEvent(pointer);
                
                taskEnded();

            }

//            }
        }catch (InterruptedException e){
//            logger.error("",e);
        }
        catch (Exception e){
            throw new DataImportException("",e);
        }
        finally {
            destroy();

        }
    }


    /**
     * 字节数组扩容
     * @param arr
     * @return
     */
    public byte[] grow(byte[] arr) {
        int len = arr.length;
        int half = len >> 1;
        int growSize = Math.max(half, 1);
        byte[] arrNew = new byte[len + growSize];
        System.arraycopy(arr, 0, arrNew, 0, len);
        return arrNew;
    }

    /**
     * 字节数组解码成字符串
     * @param arr
     * @param arrPos
     * @return
     */
    public String decode(byte[] arr, int arrPos) {
        if (arrPos == 0)
            return null;
        try {
            if(fileInfo.getCharset() != null)
                return new String(arr, 0, arrPos,fileInfo.getCharset());
            else{
                return new String(arr, 0, arrPos);
            }
        } catch (Exception e) {
          throw new FilelogPluginException("decode line data failed:",e);
        }
    }
}
