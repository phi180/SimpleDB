package simpledb.record.homework;

import simpledb.buffer.ReplacementStrategy;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.util.FileUtil;

import java.nio.charset.Charset;
import java.sql.Types;
import java.util.Date;
import java.util.Random;

public class Homework1 {
    private static Date experimentTime = new Date();

    private static final Configuration CONFIGURATION = Configuration.FOUR;

    public static void main(String[] args) throws Exception {
        FileUtil.deleteFolder("tabletest");
        if(CONFIGURATION.equals(Configuration.ONE))
            operation(new SimpleDB("tabletest", 100, 100));
        if(CONFIGURATION.equals(Configuration.TWO))
            operation(new SimpleDB("tabletest", 100, 1000));
        if(CONFIGURATION.equals(Configuration.THREE))
            operation(new SimpleDB("tabletest", 1000, 250, ReplacementStrategy.LRU));
        if(CONFIGURATION.equals(Configuration.FOUR))
            operation(new SimpleDB("tabletest", 500, 10000, ReplacementStrategy.LRU));
    }

    private static void operation(SimpleDB sdb){
        SimpleDB db = sdb;
        Transaction tx = db.newTx();

        /*-----------------*
         * Prima relazione *
         *-----------------*/
        Schema sch1= new Schema();
        sch1.addIntField("A");
        sch1.addStringField("B", 15);
        Layout layout1 = new Layout(sch1);
        for (String fldname : layout1.schema().fields()) {
            int offset = layout1.offset(fldname);
            System.out.println(fldname + " has offset " + offset);
        }
        /*-------------------*
         * Primi inserimenti *
         *-------------------*/
        byte[] array = new byte[15]; // length is bounded by 15
        TableScan ts1 = new TableScan(tx, "R", layout1);
        db.fileMgr().resetBlockStats();
        ts1.beforeFirst();
        System.out.println("Filling the table with 100'000 random records. Current block = " + ts1.getRid().blockNumber());
        for (int i=0; i<100000;  i++) {
            ts1.insert();
            ts1.setInt("A", i+1);
            new Random().nextBytes(array);
            String generatedString = new String(array, Charset.forName("UTF-8"));
            ts1.setString("B", generatedString);
        }

        if(CONFIGURATION.equals(Configuration.THREE)) {
            /* voglio forzare la scrittura dal buffer alla memoria secondaria dell'ultimo blocco scritto e ancora pinnato
            * (valido solo nella configurazione 3)*/
            sdb.bufferMgr().unpin(sdb.bufferMgr().findExistingBuffer("R.tbl", 2702));
        }

        System.out.println("First insertions: " + db.fileMgr().getBlockStats());

        /*-------------------*
         * Seconda relazione *
         *-------------------*/
        Schema sch2 = new Schema();
        sch2.addField("C", Types.BIGINT, 0);
        sch2.addIntField("D");
        sch2.addIntField("E");
        Layout layout2 = new Layout(sch2);
        for (String fldname : layout2.schema().fields()) {
            int offset = layout2.offset(fldname);
            System.out.println(fldname + " has offset " + offset);
        }
        /*---------------------*
         * Secondi inserimenti *
         *---------------------*/
        db.fileMgr().resetBlockStats();
        TableScan ts2 = new TableScan(tx, "S", layout2);
        System.out.println("After instanciating table scan ts2: " + db.fileMgr().getBlockStats());

        db.fileMgr().resetBlockStats();

        ts2.beforeFirst();
        for (int i=0; i<80000; i++) {
            ts2.insert();
            ts2.setInt("D", i);
            ts2.setInt("C", i%50);
            ts2.setInt("E", i%100);
        }

        System.out.println("Second insertions: " + db.fileMgr().getBlockStats());

        logSpentTime("Insertions completed");

        /*-------------------*
         * Prima lettura - R *
         *-------------------*/
        db.fileMgr().resetBlockStats();
        ts1.beforeFirst();
        int sum = 0;
        while (ts1.next()) {
            int fieldValue = ts1.getInt("A");
            if( fieldValue >= 1 && fieldValue <= 1000) {
                sum++;
            }
        }
        System.out.println("Read from R (1), sum: { " + sum + "}\tstats: {" + db.fileMgr().getBlockStats() + "}");

        /*---------------------*
         * Seconda lettura - R *
         *---------------------*/
        db.fileMgr().resetBlockStats();
        ts1.beforeFirst();
        sum = 0;
        while (ts1.next()) {
            int fieldValue = ts1.getInt("A");
            if(fieldValue >= 2000 && fieldValue <= 3000) {
                sum++;
            }
        }
        System.out.println("Read from R (2), sum: { " + sum + "}\tstats: {" + db.fileMgr().getBlockStats() + "}");

        /*-------------------*
         * Terza lettura - R *
         *-------------------*/
        db.fileMgr().resetBlockStats();
        ts1.beforeFirst();
        sum = 0;
        while (ts1.next()) {
            int fieldValue = ts1.getInt("A");
            if(fieldValue >= 2500 && fieldValue < 2600) {
                sum++;
            }
        }
        System.out.println("Read from R (3), sum: { " + sum + "}\tstats: {" + db.fileMgr().getBlockStats() + "}");

        /*--------------------*
         * Quarta lettura - S *
         *--------------------*/
        db.fileMgr().resetBlockStats();
        ts2.beforeFirst();
        sum = 0;
        while (ts2.next()) {
            int fieldValue = ts2.getInt("C");
            if ( fieldValue >= 1 && fieldValue <= 1000) {
                sum++;
            }
        }
        System.out.println("Read from S (1), sum: { " + sum + "}\tstats: {" + db.fileMgr().getBlockStats() + "}");



        /*-----------------*
         * Prima conta - S *
         *-----------------*/
        db.fileMgr().resetBlockStats();
        ts2.beforeFirst();
        sum = 0;
        while (ts2.next()) {
            if(ts2.getInt("D") == ts2.getInt("E")) {
                sum++;
            }
        }
        System.out.println("Count from S, sum: { " + sum + "}\tstats: {" + db.fileMgr().getBlockStats() + "}");

        ts1.close();
        ts2.close();

        logSpentTime("End of execution");
    }

    private static void logSpentTime(String logPrefix) {
        Date instantNow = new Date();

        Long deltaMilliSecs = instantNow.getTime() - experimentTime.getTime();

        System.out.println(logPrefix + ": " + deltaMilliSecs + " (ms)");
    }
}
