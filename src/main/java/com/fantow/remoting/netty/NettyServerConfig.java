package com.fantow.remoting.netty;

public class NettyServerConfig implements Cloneable {

    private int listenPort = 8888;
    private int serverWorkerThreads = 8;
    private int serverCallbackExecutorThreads = 0;

    // 不知道这个是什么
    private int serverSelectorThreads = 3;

    private int serverOnewaySemaphoreValue = 256;
    private int serverAsyncSemaphoreValue = 64;
    private int serverChannelMaxIdleTimeSeconds = 120;

    // socket发送缓冲区大小，默认为64K
    private int serverSocketSendBufSize = NettySystemConfig.socketSendBufSize;

    // socket接收缓冲区大小，默认为64K
    private int serverSocketRecvBufSize = NettySystemConfig.socketRecvBufSize;

    // ByteBuffer是否开启缓存
    private boolean serverPooledByteBufferAllocatorEnable = true;

    // 在Linux系统下，是否开始Epoll IO模型
    private boolean useEpollNativeSelector = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getServerWorkerThreads() {
        return serverWorkerThreads;
    }

    public void setServerWorkerThreads(int serverWorkerThreads) {
        this.serverWorkerThreads = serverWorkerThreads;
    }

    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public void setServerSelectorThreads(int serverSelectorThreads) {
        this.serverSelectorThreads = serverSelectorThreads;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSendBufSize() {
        return serverSocketSendBufSize;
    }

    public void setServerSocketSendBufSize(int serverSocketSendBufSize) {
        this.serverSocketSendBufSize = serverSocketSendBufSize;
    }

    public int getServerSocketRecvBufSize() {
        return serverSocketRecvBufSize;
    }

    public void setServerSocketRecvBufSize(int serverSocketRecvBufSize) {
        this.serverSocketRecvBufSize = serverSocketRecvBufSize;
    }

    public boolean isServerPooledByteBufferAllocatorEnable() {
        return serverPooledByteBufferAllocatorEnable;
    }

    public void setServerPooledByteBufferAllocatorEnable(boolean serverPooledByteBufferAllocatorEnable) {
        this.serverPooledByteBufferAllocatorEnable = serverPooledByteBufferAllocatorEnable;
    }

    public boolean isUseEpollNativeSelector() {
        return useEpollNativeSelector;
    }

    public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
        this.useEpollNativeSelector = useEpollNativeSelector;
    }

    public int getServerCallbackExecutorThreads() {
        return serverCallbackExecutorThreads;
    }

    public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
        this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
    }
}
