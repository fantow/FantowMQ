package com.fantow.remoting;

import com.fantow.remoting.exception.RemotingException;
import com.fantow.remoting.netty.NettyRequestProcessor;
import com.fantow.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;

public interface RemotingClient extends RemotingService{

    void updateNameServerAddressList(List<String> addrs);

    List<String> getNameServerAddressList();


    // 同步方式通信
    RemotingCommand invokeSync(String addr, RemotingCommand request,
                               long timeoutMillis) throws InterruptedException, RemotingException;

    // 异步方式通信
    void invokeAsync(String addr,RemotingCommand request,long timoutMiilis,
                     InvokeCallback invokeCallback) throws InterruptedException, RemotingException;

    void invokeOneway(String addr,RemotingCommand request,long timeoutMillis) throws InterruptedException, RemotingException;

    void registerProcessor(int requestCode, NettyRequestProcessor processor,
                           ExecutorService executor);

    void setCallbackExecutor(ExecutorService callbackExecutor);

    ExecutorService getCallbackExecutor();

}
