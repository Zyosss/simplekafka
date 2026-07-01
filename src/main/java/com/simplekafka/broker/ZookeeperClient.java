package com.simplekafka.broker;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is the orchestrator of all broker it does leader election and organize things between brokers
 */
public class ZookeeperClient implements Watcher {

    private static final Logger logger= Logger.getLogger(ZookeeperClient.class.getName());
    private final static int SESSION_TIMEOUT=30000;

    private final String host;
    private final int port;
    private ZooKeeper zk;
    private CountDownLatch connectedSignal=new CountDownLatch(1);

    public ZookeeperClient(String host, int port) {
        this.host=host;
        this.port=port;
    }

    /**
     * Coonect to Zookeeper
     */
    public void connect() throws IOException, InterruptedException {
        zk=new ZooKeeper(getConnectString(),SESSION_TIMEOUT,this);
        connectedSignal.await();

        createPath("/broker");
        createPath("/topic");
        createPath("/controller");
    }



    public String getConnectString(){
        return host + ":"+port;
    }

    private void createPath(String path){
        try{
            if(path.equals("/")){
                return;
            }
            int lastSlashIndex=path.lastIndexOf("/");
            if(lastSlashIndex>0){
                //create parent path
                String parentPath=path.substring(0, lastSlashIndex);
                createPath(parentPath);
            }
            if(zk.exists(path,false)==null){
                zk.create(path,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Create Zookeeper path "+path);

            }
        }catch(Exception e){
            logger.log(Level.WARNING,"Failed to create Zookeeper path "+path,e);
        }

    }


    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
