package org.darkphoenixs.hbase.repair;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeartBateRunable implements  Runnable{

    int sleepnum = 3;
    Thread currentThread = null;

    public HeartBateRunable(Thread currentThread) {
        this.currentThread = currentThread;
    }

    @Override
    public void run() {
        if(currentThread == null || !currentThread.isAlive()){
            log.info("心跳程序入参异常");
        }
        while (currentThread != null && currentThread.isAlive()){
            try {
                Thread.sleep(sleepnum*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("心跳程序每"+sleepnum+"秒心跳下");
        }
        log.info("心跳程序退出");
    }
}
