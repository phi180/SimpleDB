package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.server.SimpleDB;

public class BufferMgrReplacementStrategyTest {

    public static void main(String[] args) throws Exception {
        SimpleDB db = new SimpleDB("buffermgrtest", 400, 3); // only 3 buffers
        BufferMgr bm = db.bufferMgr();

        Buffer[] buff = new Buffer[6];
        buff[0] = bm.pin(new BlockId("testfile", 0));
        buff[1] = bm.pin(new BlockId("testfile", 1));
        buff[2] = bm.pin(new BlockId("testfile", 2));
        bm.unpin(buff[1]); buff[1] = null;
        buff[3] = bm.pin(new BlockId("testfile", 0)); // block 0 pinned twice
        buff[4] = bm.pin(new BlockId("testfile", 1)); // block 1 repinned
        System.out.println("Available buffers: " + bm.available());



    }

}
