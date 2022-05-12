package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.tx.recovery.LogRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LogReader {

    public static List<String> readLogLines(LogMgr lm) {
        List<String> logLines = new ArrayList<>();
        Iterator<byte[]> it = lm.iteratorReader();

        while(it.hasNext()) {
            byte[] bytes = it.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            logLines.add(rec.toString());
        }

        return logLines;
    }

}
