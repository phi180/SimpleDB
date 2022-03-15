package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.server.SimpleDB;

public class BufferMgrReplacementStrategyTest {

    private static final ReplacementStrategy REPLACEMENT_STRATEGY = ReplacementStrategy.LRU;

    public static void main(String[] args) throws Exception {
        SimpleDB db = new SimpleDB("buffermgrtest", 400, 4, REPLACEMENT_STRATEGY); // only 4 buffers
        BufferMgr bm = db.bufferMgr();

        Buffer b70 = null;
        Buffer b33 = null;
        Buffer b35 = null;
        Buffer b47 = null;
        Buffer b60 = null;
        Buffer b40 = null;

        // fare con i setter, senza reverse engineering per riprodurre stato iniziale
        // come implementare il modified?

        b70 = bm.pin(new BlockId("testfile", 70)); // ist1
        b40 = bm.pin(new BlockId("testfile", 40)); // ist2 -> to unpin in ist 5
        b35 = bm.pin(new BlockId("testfile", 35)); // ist3
        b70.setModified(1, 1); // ist 4
        bm.unpin(b40); // ist 5

        if(REPLACEMENT_STRATEGY.equals(ReplacementStrategy.LRU) || REPLACEMENT_STRATEGY.equals(ReplacementStrategy.CLOCK)) {
            Buffer t = bm.pin(new BlockId("testfile", 99)); // ist6
            bm.unpin(t);
        }

        b33 = bm.pin(new BlockId("testfile", 33)); // ist6
        b33 = bm.pin(new BlockId("testfile", 33)); // ist7
        b47 = bm.pin(new BlockId("testfile", 47)); // ist8
        bm.unpin(b35); // ist9

        System.out.println("Buffers content: " + bm.buffersInfo());

        bm.unpin(b70); // ist 10
        b60 = bm.pin(new BlockId("testfile", 60)); // ist 11
        b60.setModified(2,1); // ist 12
        bm.unpin(b60); // ist 13
        bm.flushAll(); // ist 14
        b60.setModified(2,0); // ist 12
        b47.setModified(4,1); // ist 15
        bm.unpin(b47); // ist 16
        b70 = bm.pin(new BlockId("testfile", 70)); // ist 17
        b70.setModified(5,1); // ist 18
        bm.unpin(b70); // ist 19
        b60 = bm.pin(new BlockId("testfile", 60)); // ist 20
        bm.unpin(b60); // ist 21
        b70 = bm.pin(new BlockId("testfile", 70)); // ist 22
        bm.unpin(b70);

        System.out.println("Buffers content: " + bm.buffersInfo());

    }

}
