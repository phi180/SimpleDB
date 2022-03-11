package simpledb.file;

import simpledb.server.SimpleDB;

import java.io.IOException;

public class BlockStatsTest {

    public static void main(String[] args) throws IOException {
        SimpleDB db = new SimpleDB("filetest", 400, 8);
        FileMgr fm = db.fileMgr();
        BlockId blk = new BlockId("testfile", 2);

        BlockStats blockStats = new BlockStats();
        blockStats.logReadBlock(blk);
        blockStats.logWrittenBlock(blk);

        System.out.println("Block stats for blockId=" + blk.number() + ": " + blockStats.toString());
    }

}
