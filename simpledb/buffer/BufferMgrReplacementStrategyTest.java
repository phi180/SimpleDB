package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.server.SimpleDB;
import simpledb.util.SimpleTimer;

public class BufferMgrReplacementStrategyTest {

    private static final ReplacementStrategy REPLACEMENT_STRATEGY = ReplacementStrategy.LRU;
    private static final Integer BUFF_SIZE = 4; // only 4 buffers
    private static final String DIR_NAME = "buffermgrtest";
    private static final Integer BLOCK_SIZE = 400;

    public static void main(String[] args) throws Exception {
        mar21Exam();

    }

    private static BufferMgr mar21Exam() {
        SimpleDB db = new SimpleDB(DIR_NAME, BLOCK_SIZE, BUFF_SIZE, REPLACEMENT_STRATEGY);
        BufferMgr bm = db.bufferMgr();

        Buffer[] buffers = new Buffer[4];
        buffers[0] = bm.pin(new BlockId("testfile", 70));
        buffers[1] = bm.pin(new BlockId("testfile", 33));
        buffers[2] = bm.pin(new BlockId("testfile", 35));
        buffers[3] = bm.pin(new BlockId("testfile", 47));

        buffers[0].setLoadTime(1L);
        buffers[0].setModified(1,1);
        buffers[1].setPins(2);
        buffers[1].setLoadTime(7L);
        buffers[2].setPins(0);
        buffers[2].setLoadTime(3L);
        buffers[2].setUnpinTime(8L);
        buffers[3].setLoadTime(9L);

        BlockId block60 = new BlockId("testfile", 60);
        BlockId block70 = new BlockId("testfile", 70);

        // STARTS HERE

        SimpleTimer.setInstant(9L);

        bm.unpin(bm.findExistingBuffer("testfile",70)); // 10
        bm.pin(block60); // 11
        bm.findExistingBuffer("testfile",60).setModified(1,1); // 12
        bm.unpin(bm.findExistingBuffer("testfile",60)); // 13
        bm.flushAll(); // 14
        bm.findExistingBuffer("testfile",47).setModified(1,1); // 15
        bm.unpin(bm.findExistingBuffer("testfile",47)); // 16
        bm.pin(block70); // 17
        bm.findExistingBuffer("testfile",70).setModified(1,1); // 18
        bm.unpin(bm.findExistingBuffer("testfile",70)); // 19
        bm.pin(block60); // 20
        bm.unpin(bm.findExistingBuffer("testfile",60)); // 21
        bm.pin(block70); // 22

        return bm;
    }

}
