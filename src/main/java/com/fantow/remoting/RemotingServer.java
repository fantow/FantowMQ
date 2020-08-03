package com.fantow.remoting;

import com.fantow.remoting.common.Pair;
import com.fantow.remoting.exception.RemotingException;
import com.fantow.remoting.netty.NettyRequestProcessor;
import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

public interface RemotingServer extends RemotingService {

    // 根据requestCode绑定processor
    void registerProcessor(int requestCode, NettyRequestProcessor processor,
                           ExecutorService executor);

    // 注册processor
    void registerDefaultProcessor(NettyRequestProcessor processor,ExecutorService executor);

    Pair<NettyRequestProcessor,ExecutorService> getProcessorPair(int requestCode);

    // 同步
    RemotingCommand invokeSync(Channel channel,RemotingCommand request,
                               long timeoutMillis) throws RemotingException, InterruptedException;
    // 异步
    void invokeAsync(Channel channel,RemotingCommand request,long timeoutMillis,
                     InvokeCallback invokeCallback) throws RemotingException;

    // oneway
    void invokeOneway(Channel channel,RemotingCommand remotingCommand,long timeoutMillis) throws RemotingException, InterruptedException;

}
