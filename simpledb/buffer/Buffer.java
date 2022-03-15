package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;
import simpledb.util.SimpleTimer;

import java.util.Date;

/**
 * An individual buffer. A databuffer wraps a page 
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified,
 * and if so, the id and lsn of the modifying transaction.
 * @author Edward Sciore
 */
public class Buffer {
   private FileMgr fm;
   private LogMgr lm;
   private Page contents;
   private BlockId blk = null;
   private int pins = 0;
   private int txnum = -1;
   private int lsn = -1;

   private Long loadTime;
   private Long unpinTime;

   public Buffer(FileMgr fm, LogMgr lm) {
      this.fm = fm;
      this.lm = lm;
      contents = new Page(fm.blockSize());
   }
   
   public Page contents() {
      return contents;
   }

   /**
    * Returns a reference to the disk block
    * allocated to the buffer.
    * @return a reference to a disk block
    */
   public BlockId block() {
      return blk;
   }

   public void setModified(int txnum, int lsn) {
      this.txnum = txnum;
      if (lsn >= 0)
         this.lsn = lsn;
   }

   /**
    * Return true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   public boolean isPinned() {
      return pins > 0;
   }
   
   public int modifyingTx() {
      return txnum;
   }

   /**
    * Reads the contents of the specified block into
    * the contents of the buffer.
    * If the buffer was dirty, then its previous contents
    * are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(BlockId b) {
      flush();
      blk = b;
      fm.read(blk, contents);
      pins = 0;

      this.unpinTime = null;
      this.loadTime = SimpleTimer.getInstant();
   }
   
   /**
    * Write the buffer to its disk block if it is dirty.
    */
   void flush() {
      if (txnum >= 0) {
         lm.flush(lsn);
         fm.write(blk, contents);
         txnum = -1;
      }
   }

   /**
    * Increase the buffer's pin count.
    */
   void pin() {
      this.unpinTime = null;
      pins++;
   }

   /**
    * Decrease the buffer's pin count.
    */
   void unpin() {
      pins--;

      if(!isPinned())
         this.unpinTime = SimpleTimer.getInstant();
   }

   public boolean isModified() {
      return this.txnum>=0;
   }

   public Long getLoadTime() {
      return this.loadTime;
   }

   public Long getUnpinTime() {
      return this.unpinTime;
   }

   @Override
   public String toString() {
      return "Buffer{" +
              "blk=" + blk +
              ", pins=" + pins +
              ", loadTime=" + loadTime +
              ", unpinTime=" + unpinTime +
              ", dirty=" + this.isModified() +
              '}';
   }
}