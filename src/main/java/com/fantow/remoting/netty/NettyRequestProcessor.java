package com.fantow.remoting.netty;

import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

public interface NettyRequestProcessor {
    RemotingCommand processRequest(ChannelHandlerContext ctx,RemotingCommand request);

    boolean rejectRequest();
}
