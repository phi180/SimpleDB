package simpledb.record;

import simpledb.buffer.ReplacementStrategy;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.util.FileUtil;

import java.nio.charset.Charset;
import java.sql.Types;
import java.util.Random;

public class Homework1 {
    public static void main(String[] args) throws Exception {
        FileUtil.deleteFolder("tabletest");
        //operation(new SimpleDB("tabletest", 100, 100));

        FileUtil.deleteFolder("tabletest");
        //operation(new SimpleDB("tabletest", 1000, 100));

        FileUtil.deleteFolder("tabletest");
        //operation(new SimpleDB("tabletest", 1000, 250, ReplacementStrategy.LRU));

        FileUtil.deleteFolder("tabletest");
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

        RID insertion1Rid = ts1.getRid();
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
        TableScan ts2 = new TableScan(tx, "S", layout2);
        db.fileMgr().resetBlockStats();
        ts2.beforeFirst();
        for (int i=0; i<80000; i++) {
            ts2.insert();
            ts2.setInt("D", i);
            ts2.setInt("C", i%50);
            ts2.setInt("E", i%100);
        }

        RID insertion2Rid = ts2.getRid();
        System.out.println("Second insertions: " + db.fileMgr().getBlockStats());
        // è 15999 e non 16000 esatto perché la pagina viene riempita, ma non si passa alla successiva
        // quindi i dati restano in memoria primaria

        /*-------------------*
         * Prima lettura - R *
         *-------------------*/
        db.fileMgr().resetBlockStats();
        ts1.beforeFirst();
        // un solo blocco scritto, questo perché dal tablescan ts1 precedente il puntatore era
        // rimasto bloccato sull'ultimo blocco, con il beforefirst scrive l'ultimo blocco
        // in mem secondaria e poi si riposiziona su primo blocco
        int sum = 0;
        while (ts1.next()) {
            int fieldValue = ts1.getInt("A");
            if( fieldValue >= 1 && fieldValue <= 1000) {
                sum++;
            }
        }
        System.out.println("Read from R (1), sum: { " + sum + "}\tstats: {" + db.fileMgr().getBlockStats() + "}");
        RID read1Rid = ts1.getRid();

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
        RID read2Rid = ts1.getRid();

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
        RID read3Rid = ts1.getRid();

        /*--------------------*
         * Quarta lettura - S *
         *--------------------*/
        /*db.fileMgr().resetBlockStats();
        ts2.beforeFirst();
        sum = 0;
        try {
            while (ts2.next()) {
                int fieldValue = ts2.getInt("A");
                if ( fieldValue >= 1 && fieldValue <= 1000) {
                    sum++;
                }
            }
            System.out.println("Read from S (1), sum: { " + sum + "}\tstats: {" + db.fileMgr().getBlockStats() + "}");
        } catch (Exception e){
            System.out.println("ERROR - 'A' is a field of relation 'S': " + ts2.hasField("A"));
        }*/
        RID read4Rid = ts2.getRid();


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
        RID count1Rid = ts2.getRid();
        // 1 scrittura = perché viene inserito in memoria secondaria l'ultimo blocco che non era stato
        // trascritto, poi letti 16000 perché oltre ai 15999 si agggiunge il blocco scritto

        ts1.close();
        ts2.close();
    }
}
