package ank.hao.kafka;

import org.junit.Test;

import java.util.Random;

public class Demo {

    @Test
    public void test(){
        for(int i=0;i<30;i++)
            System.out.println(new Random().nextInt(3));
    }
}
