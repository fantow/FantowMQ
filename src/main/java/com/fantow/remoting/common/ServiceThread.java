package com.fantow.remoting.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 需要继承该抽象类的类自己实现run()方法
public abstract class ServiceThread implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(ServiceThread.class);
    private static final long JOIN_TIME = 90 * 1000;
    public final Thread thread;
    public volatile boolean hasNotified = false;
    public volatile boolean stopped = false;

    public ServiceThread() {
        this.thread = new Thread(this,this.getServiceName());
    }

    public abstract String getServiceName();

    public void start(){
        this.thread.start();
    }

    public void shutdown(){
        this.shutdown(false);
    }

    // 如果传入true，表示会中断该线程。一般会传入false
    public void shutdown(boolean interrupt){
        this.stopped = true;
        logger.info("shutdown thread: " + this.getServiceName() + " interrupt " + interrupt);
        synchronized (this){
            // 释放线程
            if(!this.hasNotified){
                this.hasNotified = true;
                // 唤醒一个正处于等待的线程
                this.notify();
            }
        }

        try{
            if(interrupt){
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            // 等待该线程die，最大超时时间joinTime\
            // 等待该线程结束，才会执行下面操作
            this.thread.join(this.getJointime());
            long elapsedTime = System.currentTimeMillis() - beginTime;
            logger.info("join thread " + this.getServiceName());
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }

    }

    public long getJointime(){
        return JOIN_TIME;
    }

    public boolean isStopped(){
        return stopped;
    }

}
