package com.fantow.remoting;

import com.fantow.remoting.netty.ResponseFuture;

public interface InvokeCallback {
    void operationComplete(ResponseFuture responseFuture);
}
