package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;
import simpledb.util.DateUtils;

import java.util.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
public class BufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private static final long MAX_TIME = 10000; // 10 seconds

   private ReplacementStrategy replacementStrategy = ReplacementStrategy.NAIF;
   private int lastReplacedBufferIndex = -1;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} object.
    * @param numbuffs the number of buffer slots to allocate
    */
   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer(fm, lm);
   }

   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs, ReplacementStrategy replacementStrategy) {
      this(fm, lm, numbuffs);
      this.replacementStrategy = replacementStrategy;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   public synchronized int available() {
      return numAvailable;
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   public synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.modifyingTx() == txnum)
         buff.flush();
   }

   /**
    * Flushes the dirty buffers, not depending by modifying transaction
    */
   public synchronized void flushAll() {
      for (Buffer buff : bufferpool)
         buff.flush();
   }
   
   
   /**
    * Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * @param buff the buffer to be unpinned
    */
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         numAvailable++;
         notifyAll();
      }
   }
   
   /**
    * Pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed 
    * time period, then a {@link BufferAbortException} is thrown.
    * @param blk a reference to a disk block
    * @return the buffer pinned to that block
    */
   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);
         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }
         if (buff == null)
            throw new BufferAbortException();
         return buff;
      }
      catch(InterruptedException e) {
         throw new BufferAbortException();
      }
   }  
   
   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }
   
   /**
    * Tries to pin a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   private Buffer tryToPin(BlockId blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }

   private Buffer findExistingBuffer(BlockId blk) {
      for (Buffer buff : bufferpool) {
         BlockId b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
   }
   
   private Buffer chooseUnpinnedBuffer() {
      if(replacementStrategy.equals(ReplacementStrategy.NAIF))
         return naif();
      else if(replacementStrategy.equals(ReplacementStrategy.CLOCK))
         return clock();
      else if(replacementStrategy.equals(ReplacementStrategy.FIFO))
         return fifo();
      else if(replacementStrategy.equals(ReplacementStrategy.LRU))
         return lru();

      throw new RuntimeException("Invalid replacement strategy");
   }

   private Buffer naif() {
      int i = 0;
      for (Buffer buff : bufferpool) {
         if (!buff.isPinned()) {
            this.lastReplacedBufferIndex = i;
            return buff;
         }
         i = i+1;
      }
      return null;
   }

   private Buffer fifo() {
      Buffer choosen = Arrays.asList(this.bufferpool).stream()
              .filter(buff -> !buff.isPinned())
              .min(Comparator.comparing(buff -> buff.getLoadTime()!=null?buff.getLoadTime():-1L))
              .orElse(null);

      return choosen;
   }

   private Buffer lru() {
      for(Buffer buffer : this.bufferpool) {
         if (buffer.getLoadTime() == null && Boolean.FALSE.equals(buffer.isPinned())) {
            return buffer;
         }
      }

      return Arrays.stream(this.bufferpool)
              .filter(buff -> Boolean.FALSE.equals(buff.isPinned()))
              .min(Comparator.comparing(Buffer::getUnpinTime))
              .orElse(null);
   }

   private Buffer clock() {
      int numBuffs = this.bufferpool.length;
      int index = (this.lastReplacedBufferIndex + 1)%numBuffs;

      while(index != this.lastReplacedBufferIndex) {
         Buffer buff = this.bufferpool[index];
         if (!buff.isPinned()) {
            this.lastReplacedBufferIndex = index;
            return buff;
         }
         index = (index+1)%numBuffs;
      }
      return null;
   }

   public String buffersInfo() {
      StringBuilder sb = new StringBuilder();
      int i = 0;
      for(Buffer buffer: bufferpool) {
         sb.append("Buffer " + i + "= " + buffer.toString());
         i++;
      }
      return sb.toString();
   }

}
