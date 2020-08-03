package com.fantow.remoting;

import io.netty.channel.Channel;

// 事件监听器
public interface ChannelEventListener {

    void onChannelConnect(String remoteAddr, Channel channel);

    void onChannelClose(String remoteAddr,Channel channel);

    void onChannelException(String remoteAddr,Channel channel);

    void onChannelIdle(String remoteAddr,Channel channel);

}
