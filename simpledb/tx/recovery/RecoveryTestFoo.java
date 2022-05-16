package simpledb.tx.recovery;

import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class RecoveryTestFoo {

    public static FileMgr fm;
    public static BufferMgr bm;
    private static SimpleDB db;
    private static BlockId blk0, blk1;

    private static LogMgr lm;

    public static void main(String[] args) throws Exception {
        db = new SimpleDB("recoverytest", 400, 8);
        fm = db.fileMgr();
        bm = db.bufferMgr();
        lm = db.logMgr();
        blk0 = new BlockId("testfile", 0);
        blk1 = new BlockId("testfile", 1);

        //printLog();

        if (fm.length("testfile") == 0) {
            init();
        }
        else {
            recover();
        }

        printLog();
    }

    private static void init() {
        Transaction tx1 = db.newTx();
        tx1.pin(blk0);
        int pos = 0;
        tx1.setInt(blk0, pos, 10, true);
        tx1.commit();
        printValues("After transaction 1:");
    }

    private static void recover() {
        Transaction tx = db.newTx();
        tx.recover();
        printValues("After recovery:");
    }

    // Print the values that made it to disk.
    private static void printValues(String msg) {
        System.out.println(msg);
        Page p0 = new Page(fm.blockSize());
        Page p1 = new Page(fm.blockSize());
        fm.read(blk0, p0);
        fm.read(blk1, p1);
        int pos = 0;
        for (int i=0; i<6; i++) {
            System.out.print(p0.getInt(pos) + " ");
            System.out.print(p1.getInt(pos) + " ");
            pos += Integer.BYTES;
        }
        System.out.print(p0.getString(30) + " ");
        System.out.print(p1.getString(30) + " ");
        System.out.println();
    }

    private static void printLog() {
        System.out.println(lm.printLog()+"\n");
    }

}
