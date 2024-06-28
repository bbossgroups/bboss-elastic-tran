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

import org.frameworkset.tran.Record;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/6/26
 */
public class ReadMetric {
    private boolean collectStarted ;
    private boolean enableMultiLine ;

    private boolean  reachEOFClosed;
    private int readLines = 0;
    

    private long startPointer ;
    private long prePointer = 0;
//    private boolean firstReader;


    /**
     * 多行类型记录第一行标记
     */
    private boolean multiFirstRow ;

    /**
     * 标记多行记录开始
     */
    private boolean multilineBegin;
    private List<Record> recordList;

    public boolean isCollectStarted() {
        return collectStarted;
    }

    public void setCollectStarted(boolean collectStarted) {
        this.collectStarted = collectStarted;
    }

    /**
     * 最新的行
     */
    private FileReaderTask.Line line;
    public boolean isReachEOFClosed() {
        return reachEOFClosed;
    }

    public void setMultilineBegin(boolean multilineBegin) {
        this.multilineBegin = multilineBegin;
    }

    public boolean isMultilineBegin() {
        return multilineBegin;
    }
    //    public void setFirstReader(boolean firstReader) {
//        this.firstReader = firstReader;
//    }

    public void setReachEOFClosed(boolean reachEOFClosed) {
        this.reachEOFClosed = reachEOFClosed;
    }

    public int getReadLines() {
        return readLines;
    }

    public void setReadLines(int readLines) {
        this.readLines = readLines;
    }

    public long getStartPointer() {
        return startPointer;
    }

    public void setStartPointer(long startPointer) {
        this.startPointer = startPointer;
    }
    public int increamentReadLines(){
        readLines ++;
        return readLines;
    }

    public long getPrePointer() {
        return prePointer;
    }

    public void setPrePointer(long prePointer) {
        this.prePointer = prePointer;
    }

 


    public boolean isMultiFirstRow() {
        return multiFirstRow;
    }

    public void setMultiFirstRow(boolean multiFirstRow) {
        this.multiFirstRow = multiFirstRow;
    }

//    public boolean isFirstReader() {
//        return firstReader;
//    }

    public List<Record> getRecordList() {
        return recordList;
    }

    public void setRecordList(List<Record> recordList) {
        this.recordList = recordList;
    }
    public int recordSize(){
        return recordList.size();
    }

    public FileReaderTask.Line getLine() {
        return line;
    }

    public void setLine(FileReaderTask.Line line) {
        this.line = line;
    }

    public boolean isEnableMultiLine() {
        return enableMultiLine;
    }

    public void setEnableMultiLine(boolean enableMultiLine) {
        this.enableMultiLine = enableMultiLine;
    }
}
