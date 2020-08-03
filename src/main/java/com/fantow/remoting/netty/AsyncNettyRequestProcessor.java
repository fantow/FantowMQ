package com.fantow.remoting.netty;

import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

public abstract class AsyncNettyRequestProcessor implements NettyRequestProcessor {

    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand cmd,
                                    RemotingResponseCallback callback){
        RemotingCommand response = processRequest(ctx, cmd);
        callback.callback(response);
    }
}
