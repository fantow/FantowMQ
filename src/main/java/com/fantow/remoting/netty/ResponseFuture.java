package com.fantow.remoting.netty;

import com.fantow.remoting.InvokeCallback;
import com.fantow.remoting.common.SemaphoreReleaseOnce;
import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResponseFuture {
    // 通过这个连接request --> response
    private final int opaque;
    private final Channel processChannel;
    private final long timeoutMillis;
    private final InvokeCallback invokeCallback;

    // 将创建该对象时记录开始时间。
    private final long beginTimestamp = System.currentTimeMillis();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final SemaphoreReleaseOnce once;


    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    private volatile RemotingCommand responseCommand;
    private volatile boolean sendRequestOk = true;
    private volatile Throwable cause;




    public ResponseFuture(Channel channel,int opaque,long timeoutMillis,InvokeCallback invokeCallback,SemaphoreReleaseOnce once){
        this.opaque = opaque;
        this.processChannel = channel;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    // 通过自旋锁保证callback方法只被实现一次，具体的callback逻辑
    // operationComplete()由实现类实现
    public void executeInvokeCallback(){
        if(invokeCallback != null){
            if(this.executeCallbackOnlyOnce.compareAndSet(false,true)){
                invokeCallback.operationComplete(this);
            }
        }
    }

    public void release(){
        if(once != null){
            this.once.release();
        }
    }


    public boolean isTimeout(){
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    public RemotingCommand waitResponse(long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public void putResponse(RemotingCommand responseCommand){
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public boolean isSendRequestOK() {
        return sendRequestOk;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public int getOpaque() {
        return opaque;
    }

    public Channel getProcessChannel() {
        return processChannel;
    }

    public void setSendRequestOk(boolean sendRequestOk) {
        this.sendRequestOk = sendRequestOk;
    }

    @Override
    public String toString() {
        return "ResponseFuture{" +
                "opaque=" + opaque +
                ", processChannel=" + processChannel +
                ", timeoutMillis=" + timeoutMillis +
                ", invokeCallback=" + invokeCallback +
                ", beginTimestamp=" + beginTimestamp +
                ", countDownLatch=" + countDownLatch +
                ", executeCallbackOnlyOnce=" + executeCallbackOnlyOnce +
                ", responseCommand=" + responseCommand +
                ", sendRequestOk=" + sendRequestOk +
                ", cause=" + cause +
                '}';
    }
}
