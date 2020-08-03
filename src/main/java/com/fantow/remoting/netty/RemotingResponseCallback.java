package com.fantow.remoting.netty;

import com.fantow.remoting.protocol.RemotingCommand;

public interface RemotingResponseCallback {
    void callback(RemotingCommand response);
}
