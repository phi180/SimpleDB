package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ForwardLogIterator implements Iterator<byte[]> {

    private FileMgr fm;
    private BlockId blk;
    private Page p;

    private List<Integer> offsets = new ArrayList<>();

    /**
     * Creates an iterator for the records in the log file,
     * positioned after the last log record.
     */
    public ForwardLogIterator(FileMgr fm, BlockId blk, int startBoundary) {
        this.fm = fm;
        this.blk = blk;
        byte[] b = new byte[fm.blockSize()];
        p = new Page(b);
        moveToBlock(blk);
        initOffsets();
        this.offsets = this.offsets.stream().filter(offset -> offset <= startBoundary).collect(Collectors.toList());
    }

    public ForwardLogIterator(FileMgr fm, BlockId blk) {
        this(fm,blk,fm.blockSize());
    }

    public boolean hasNext() {
        if(blk.number()==fm.length("simpledb.log")-1 && offsets.isEmpty())
            return false;

        return blk.number()<fm.length("simpledb.log");
    }

    public byte[] next() {
        if (offsets.isEmpty()) {
            blk = new BlockId(blk.fileName(), blk.number()+1);
            moveToBlock(blk);
            initOffsets();
        }

        int currentpos = offsets.remove(offsets.size()-1);
        byte[] rec = p.getBytes(currentpos);
        return rec;
    }

    /**
     * Moves to the specified log block
     * and positions it at the first record in that block
     * (i.e., the most recent one).
     */
    private void moveToBlock(BlockId blk) {
        fm.read(blk, p);
    }

    private void initOffsets() {
        this.offsets = new ArrayList<>();
        int tempOffset = p.getInt(0);
        offsets.add(tempOffset);
        while(tempOffset>=0 && tempOffset<fm.blockSize()) {
            tempOffset += Integer.BYTES + p.getBytes(tempOffset).length;
            offsets.add(tempOffset);
        }

        offsets.remove(offsets.size()-1);
    }
}
