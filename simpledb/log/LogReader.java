package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.tx.recovery.LogRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class LogReader {


    public static void printLogLines(LogMgr lm) {
        List<String> logLines = new ArrayList<>();
        Iterator<byte[]> it = lm.iterator2();

        while(it.hasNext()) {
            byte[] bytes = it.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            logLines.add(rec.toString());
        }

        Collections.reverse(logLines);
        for(String line:logLines) {
            System.out.println(line);
        }
    }

}
