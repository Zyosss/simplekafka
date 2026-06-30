package com.simplekafka.broker;

//this class define how broker and client communicate over the network

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Defines the wire protocol for SimpleKafka communication
 * Converts between Java objects and binary format for network transmission
 */
public class Protocol {

    //client request types(0x01 to 0x0F)
    public static final byte PRODUCE=0x01; //send message
    public static final byte FETCH=0x02; //get message
    public static final byte  METADATA=0x03; //GET Cluster
    public static final byte CREATE_TOPIC=0x04; //create new topic


    //Broker response type (0x10 to 0x1F)
    public static final byte PRODUCE_RESPONSE=0x11;
    public static final byte FETCH_RESPONSE=0x12;
    public static final byte METADATA_RESPONSE=0x13;
    public static final byte ERROR_RESPONSE=0x1F;

    //Internal broker Communication
    public static final byte REPLICATE=0x21;
    public static final byte REPLICATE_ACK=0x22;
    public static final byte TOPIC_NOTIFICATION=0x23;


    public static void sendErrorResponse(SocketChannel channel ,String errormessage) throws IOException {
        ByteBuffer buffer =ByteBuffer.allocate(3+errormessage.length());
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short)errormessage.length());
        buffer.put(errormessage.getBytes());
        buffer.flip();
        channel.write(buffer);
    }

    /** Encode a Producer Request
     * */
     public static ByteBuffer encodeProduceRequest(String topic, int partition,byte[] message){

         /**
          * We allocate 11 bytes because the ( size of produce_response ) 1 byte+
          * 2 bytes contain topic string length+
          * Next 4 bytes contain partition ID +
          * Next 4 bytes contain message length,
          * and we add after it
          * Next N bytes contain the topic name
          * */
    ByteBuffer buffer=ByteBuffer.allocate(11+topic.length()+message.length);

         /**
          * First byte identifies request type (0x01)
          * Next 2 bytes contain topic string length
          * Next N bytes contain the topic name
          * Next 4 bytes contain partition ID
          * Next 4 bytes contain message length
          * Remaining bytes contain the message
          *
          */
    buffer.put(PRODUCE_RESPONSE);
    buffer.putShort((short)topic.length());
    buffer.put(topic.getBytes());
    buffer.putInt(partition);
    buffer.putInt(message.length);
    buffer.put(message);
    return buffer;

     }

    /** Encode a Fetch Request
     * */

    public static ByteBuffer encodeFetchRequest(String topic ,int partition,long offset,int maxBytes){

        ByteBuffer buffer=ByteBuffer.allocate(19+topic.length());
        buffer.put(FETCH);
        buffer.putShort((short)topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest(){
         ByteBuffer buffer=ByteBuffer.allocate(1);
         buffer.put(METADATA);
         buffer.flip();
         return buffer;
    }

    /**
     *Encode topic Request
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic,int numPartitions,short replicationFactor){

        ByteBuffer buffer=ByteBuffer.allocate(9+topic.length());
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short)topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;

    }

    /**
     *Encode Replication Request For BACKUP in case of borker is down
     * */

    public static ByteBuffer encodeReplicateRequest(String topic,int partition,long offset,byte[] message){
        ByteBuffer buffer=ByteBuffer.allocate(17+topic.length()+message.length);
        buffer.put(REPLICATE);
        buffer.putShort((short)topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode Topic Notification
     */

    public static ByteBuffer encodeTopicNotification(String topic){
        ByteBuffer buffer=ByteBuffer.allocate(3+topic.length());
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short)topic.length());
        buffer.put(topic.getBytes());
        buffer.flip();
        return buffer;
    }

    /**
     * Produce Result class
     */
    public static class ProduceResult{

    private final long offset;
    private final String error;

    public ProduceResult(long offset,String error){
        this.offset=offset;
        this.error=error;
    }

    public long getOffset() {
    return offset;
    }
    public String getError() {
        return error;
    }
    public boolean isSuccess(){
        return error==null;
    }

    }

    /**
     * Result class for Fetch
     */
}

