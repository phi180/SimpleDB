package simpledb.file;

import java.util.HashMap;
import java.util.Map;

public class BlockStats {

    private Map<String, BlockStatDetails> blockStats = new HashMap<>();

    public void logReadBlock(BlockId block) {
        this.blockStats.putIfAbsent(block.fileName(), new BlockStatDetails());
        this.blockStats.get(block.fileName()).increaseRead();
    }

    public void logWrittenBlock(BlockId block) {
        this.blockStats.putIfAbsent(block.fileName(), new BlockStatDetails());
        this.blockStats.get(block.fileName()).increaseWritten();
    }

    public void reset() {
        this.blockStats = new HashMap<>();
    }

    @Override
    public String toString() {
        return "BlockStats{" +
                "blockStats=" + blockStats +
                '}';
    }

    class BlockStatDetails {
        private int read;
        private int written;

        public int getRead() {
            return read;
        }

        public void setRead(int read) {
            this.read = read;
        }

        public int getWritten() {
            return written;
        }

        public void setWritten(int written) {
            this.written = written;
        }

        public void increaseRead() {
            this.read++;
        }

        public void increaseWritten() {
            this.written++;
        }

        @Override
        public String toString() {
            return "BlockStatDetails{" +
                    "read=" + read +
                    ", written=" + written +
                    '}';
        }
    }
}
