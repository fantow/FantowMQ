package com.fantow.remoting;

import com.fantow.remoting.protocol.RPCHook;

// 抽象了Remoting提供的服务
public interface RemotingService {

    void start();

    void shutdown();

    void registerRPCHook(RPCHook rpcHook);

}
