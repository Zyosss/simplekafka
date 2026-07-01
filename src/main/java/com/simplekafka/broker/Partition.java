package com.simplekafka.broker;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Fundemantal unit data storage in kafka
 */
public class Partition {


    public static final Logger LOGGER=Logger.getLogger(Partition.class.getName());
    private static final int DEFAULT_SEGMENT_SIZE=1024*1024; //1 mb size
    private static final String LOG_SUFFIX=".log";
    private static final String INDEX_SUFFIX=".index";


    private final int id;                     // Unique partition identifier
    private int leader;                       // Leader broker ID
    private List<Integer> followers;          // Follower broker IDs for replication
    private final String baseDir;             // Directory for log storage
    private final AtomicLong nextOffset;      // Next available message offset
    private final ReadWriteLock lock;         // Concurrency control mechanism
    private RandomAccessFile activeLogFile;   // Currently active log file
    private FileChannel activeLogChannel;     // Channel for file operations
    private final List<SegmentInfo> segments; // List of segments in the partition



    public Partition(int id,int leader,List<Integer> followers,String baseDir) {

        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        initialize();
    }

    private void initialize() {
        // Open the last segment for appending
        try{
            // Create directory if needed
            File dir = new File(baseDir);
            if(!dir.exists()){
                dir.mkdirs();
            }
            // Load existing segments
            File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
            if(files !=null && files.length>0){
                for(File file : files){
                    String baseName =file.getName().substring(0, file.getName().length()-LOG_SUFFIX.length());
                    long baseOffset=Long.parseLong(baseName);

                    File indexFile=new File(baseDir,baseName+INDEX_SUFFIX);
                    if(!indexFile.exists()){
                        SegmentInfo segment=new SegmentInfo(baseOffset,file.getAbsolutePath(),indexFile.getAbsolutePath());
                        segments.add(segment);
                    }
                }

                // Sort segments by offset
                segments.sort((s1,s2)->Long.compare(s1.getBaseOffset(),s2.getBaseOffset()));
                // Determine next available offset
                if(!segments.isEmpty()){
                    SegmentInfo lastSegment=segments.get(segments.size()-1);
                    nextOffset.set(lastSegment.getBaseOffset()+countMessagesInSegment(lastSegment));
                }

            }
            // Create a new segment if none exists
            if(segments.isEmpty()){
                createNewSegment(0);
            }
            else {
                //Open last segment as active
                SegmentInfo lastSegment=segments.get(segments.size()-1);
                openSegmentForAppend(lastSegment);
            }
            LOGGER.info("Initialized partition " + id + " with " + segments.size() +
                    " segments, next offset: " + nextOffset.get());

        }
        catch(Exception e){
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);

        }
    }

    private void createNewSegment(long baseOffset)throws IOException{
        String baseName =String.format("%20d",baseOffset);
        String logPath=baseDir+File.separator+baseName+LOG_SUFFIX;
        String indexPath=baseDir+File.separator+baseName+INDEX_SUFFIX;

        //Create log File
        File logFile=new File(logPath);
        logFile.createNewFile();

        //Create Index File
        File indexFile=new File(indexPath);
        indexFile.createNewFile();

        //Add to segment list
        SegmentInfo segment=new SegmentInfo(baseOffset,logPath,indexPath);
        segments.add(segment);

        //Open for append
        openSegmentForAppend(segment);
    LOGGER.info("Created new Segment "+id+",base offset: "+baseOffset);

    }

    /**
     *Open Segment for append operation
     */
    private void  openSegmentForAppend(SegmentInfo segment)throws IOException{
        //Close the currently active segment if any
        if(activeLogChannel !=null && activeLogChannel.isOpen()){
            activeLogChannel.close();
        }
        if(activeLogFile!=null){
            activeLogFile.close();
        }
        //Open the segment
        activeLogFile=new RandomAccessFile(segment.getLogPath(),"rw");
        activeLogChannel=activeLogFile.getChannel();

        //Move to the end of the file for appending
        activeLogChannel.position(activeLogChannel.size());


    }


    private long countMessagesInSegment(SegmentInfo segment)throws IOException{
        long count=0;
        try(RandomAccessFile logFile=new RandomAccessFile(segment.getLogPath(),"r");
            FileChannel logChannel=logFile.getChannel()){
                ByteBuffer buffer=ByteBuffer.allocate(4); //size of field 4 bytes

            while(logChannel.position() <logChannel.size()){
                buffer.clear();
                int bytesRead=logChannel.read(buffer);
                if(bytesRead<4) break;

                buffer.flip();
                int messageSize=buffer.getInt();
                //skip message
                logChannel.position(logChannel.position()+messageSize);
                count++;
            }
        }
        return count;
    }


    private static class SegmentInfo {
        private final long baseOffset;
        private final String logPath;
        private final String indexPath;

        public SegmentInfo(long baseOffset, String logPath, String indexPath) {
            this.baseOffset = baseOffset;
            this.logPath = logPath;
            this.indexPath = indexPath;
        }
        public long getBaseOffset() {
            return baseOffset;
        }
        public String getLogPath() {
            return logPath;
        }
        public String getIndexPath() {
            return indexPath;
        }


    }
}



