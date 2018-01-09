//package com.za.apps.zk;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.zookeeper.KeeperException;
//import org.apache.zookeeper.WatchedEvent;
//import org.apache.zookeeper.Watcher;
//import org.apache.zookeeper.ZooKeeper;
//import org.apache.zookeeper.data.Stat;
//import org.junit.After;
//import org.junit.Before;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//
//public class ZookeeperHelper {
//    private static final int SESSION_TIMEOUT = 30000;
//
//    public static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperHelper.class);
//
//    private Watcher watcher =  new Watcher() {
//
//        public void process(WatchedEvent event) {
//            LOGGER.info("process : " + event.getType());
//        }
//    };
//
//    private ZooKeeper zooKeeper;
//
//    public ZookeeperHelper(){
//        try {
//            connect();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     *  连接zookeeper
//     * <br>------------------------------<br>
//     * @throws IOException
//     */
//    public void connect() throws IOException {
//        zooKeeper  = new ZooKeeper("master:2181,slave1:2181,slave2:2181", SESSION_TIMEOUT, watcher);
//    }
//
//    /**
//     *  关闭连接
//     * <br>------------------------------<br>
//     */
//    public void close() {
//        try {
//            zooKeeper.close();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * 是否存在
//     * @param path
//     * @return
//     */
//    public boolean exists(String path){
//        try {
//            Stat data = zooKeeper.exists(path,false);
//            if(null != data) return data.getCzxid()>0;
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return false;
//    }
//
//    /**
//     *获取offset
//     * @param path
//     * @return
//     */
//    public String getOffsets(String path){
//        try {
//            if(exists(path)) {
//                return new String(zooKeeper.getData(path, null, null));
//            }
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return "";
//    }
//
//    /**
//     * 获取Lag
//     * @param path
//     * @param host
//     * @param port
//     * @param topic
//     * @param partition
//     * @return
//     */
//    public long getLag(String path,String host,int port,String topic,int partition){
//        String offsetstr = getOffsets(path);
//        if(!StringUtils.isEmpty(offsetstr)){
//            long offsets = Long.parseLong(offsetstr);
//            long logsize = KafkaHelper.getLogSize(host,port,topic,partition);
//            return logsize-offsets;
//        }
//        return 0;
//    }
//
//
//    public static  void main(String[] args) throws Exception {
//        ZookeeperHelper zk = new ZookeeperHelper();
//        long lag =zk.getLag("/consumers/task_crawler_reader/offsets/crawler_tyc_sort_item/0","master",9092,"crawler_tyc_sort_item",0);
//        System.out.println(lag);
//        zk.close();
//    }
//
//}
