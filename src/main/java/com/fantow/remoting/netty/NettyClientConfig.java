package com.fantow.remoting.netty;

public class NettyClientConfig {

    private int clientWorkerThread = 4;
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;
    private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;
    private int connectionTimeoutMillis = 3000;

    private boolean useTLS;

    private int clientChannelMaxIdleTimeSeconds = 120;

    private int clientSocketSendBufSize = NettySystemConfig.socketSendBufSize;
    private int clientSocketRecvBufSize = NettySystemConfig.socketRecvBufSize;


    public int getClientWorkerThread() {
        return clientWorkerThread;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public int getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }

    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }

    public int getConnectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public int getClientSocketSendBufSize() {
        return clientSocketSendBufSize;
    }

    public int getClientSocketRecvBufSize() {
        return clientSocketRecvBufSize;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }
}
