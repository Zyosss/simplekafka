package com.simplekafka.broker;

//this class defice how broker and client communicate over the network

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
}
