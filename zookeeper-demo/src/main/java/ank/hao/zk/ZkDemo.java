package ank.hao.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZkDemo implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) {

        try {
            ZooKeeper zooKeeper = new ZooKeeper("ank.centos1:2181/haoak-test", 5000, new ZkDemo());
            System.out.println(zooKeeper.getState());
            long sessionId = zooKeeper.getSessionId();
            byte[] sessionPwd = zooKeeper.getSessionPasswd();
            connectedSemaphore.await();
            System.out.println("Zookeeper session established.");

            //创建节点
//            zooKeeper.create("/1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
//                    (int i, String s, Object o, String s1) -> {
//                        System.out.println("String callback..");
//                }, null);
//
//            TimeUnit.SECONDS.sleep(5);

            //删除节点
//            zooKeeper.delete("/1", 1,
//                    (int i, String s, Object o) -> {
//                        System.out.println("void callback..");
//
//                }, null);
//
//            TimeUnit.SECONDS.sleep(5);

            //查询节点
//            List<String> list = zooKeeper.getChildren("/", false);
//            System.out.println(list);
//            for(String child:list){
//                byte[] bytes = zooKeeper.getData("/"+child,false,null);
//                System.out.println(new String(bytes));
//                zooKeeper.delete("/"+child,1);
//            }

            //zooKeeper.close();
            //验证session复用
//            TimeUnit.SECONDS.sleep(5);
//
//            ZooKeeper zooKeeper1 = new ZooKeeper("ank.centos1:2181", 5000, new ZkDemo(), sessionId, sessionPwd, false);
//            System.out.println(zooKeeper1.getState());
//
//            TimeUnit.SECONDS.sleep(5);
//
//            ZooKeeper zooKeeper2 = new ZooKeeper("ank.centos1:2181", 5000, new ZkDemo(), 11, new byte[]{}, false);
//            System.out.println(zooKeeper2.getState());
//
//            TimeUnit.SECONDS.sleep(5);
            TimeUnit.MINUTES.sleep(5);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
           e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("Receive watched event: "+watchedEvent);
        if(Event.KeeperState.SyncConnected==watchedEvent.getState()){
            connectedSemaphore.countDown();
        }
    }
}
