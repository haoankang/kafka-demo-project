package ank.hao.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CuratorDemo {

    public static void main(String[] args) throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(1000,3000,3);
        //CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("192.168.137.200:2181,192.168.137.200:2182,192.168.137.200:2183",retryPolicy);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString("192.168.137.200:2181,192.168.137.200:2182,192.168.137.200:2183")
                .retryPolicy(retryPolicy).connectionStateListenerDecorator((CuratorFramework var1, ConnectionStateListener var2)->{

                    return var2;
                }).build();
        curatorFramework.start();
        //创建节点
        //异步
//        curatorFramework.create().withMode(CreateMode.PERSISTENT).inBackground((CuratorFramework var1, CuratorEvent var2)->{
//            System.out.println(var2.getWatchedEvent());
//            countDownLatch.countDown();
//        }).forPath("/haoak-test","test".getBytes());
//
//        countDownLatch.await();

        //监听节点
        NodeCache nodeCache = new NodeCache(curatorFramework, "/haoak-test", false);
        nodeCache.start(true);
        nodeCache.getListenable().addListener(()->{
            System.out.println("Node data update, new data: "+new String(nodeCache.getCurrentData().getData()));
        });
        for(int i=0;i<5;i++){
            curatorFramework.setData().forPath("/haoak-test", ("hahah"+i).getBytes());
            TimeUnit.SECONDS.sleep(3);
        }

        //监听子节点
        PathChildrenCache childrenCache = new PathChildrenCache(curatorFramework, "/haoak-test", false);
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        childrenCache.getListenable().addListener((CuratorFramework var1, PathChildrenCacheEvent var2)->{
            System.out.println("Node child update, event: "+var2.getType().name());
        });

        for(int i=0;i<3;i++){
            curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath("/haoak-test/"+i);
            TimeUnit.SECONDS.sleep(2);
        }

        //Master选举
        //分布式锁
        //分布式计数器
        //...

        List<String> list = curatorFramework.getChildren().forPath("/");
        if(!CollectionUtils.isEmpty(list)){
            for(String str:list){
                System.out.println("path: "+str);
                System.out.println("data: "+new String(curatorFramework.getData().forPath("/"+str)));
            }
        }
        //更改节点值
//        curatorFramework.setData().inBackground(
//                (CuratorFramework var1, CuratorEvent var2)->{
//                    System.out.println(var2.getWatchedEvent());
//                }).forPath("/haoak-test","hak0821".getBytes());

        TimeUnit.SECONDS.sleep(4);

        curatorFramework.close();
    }
}
