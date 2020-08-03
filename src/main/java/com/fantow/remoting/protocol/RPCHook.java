package com.fantow.remoting.protocol;

public interface RPCHook {
    void doBeforeRequest(String remoteAddr,RemotingCommand request);

    void doAfterRequest(String remoteAddr,RemotingCommand request,RemotingCommand response);
}
