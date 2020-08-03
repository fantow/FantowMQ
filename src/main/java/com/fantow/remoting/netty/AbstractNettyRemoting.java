package com.fantow.remoting.netty;

import com.fantow.remoting.ChannelEventListener;
import com.fantow.remoting.InvokeCallback;
import com.fantow.remoting.common.Pair;
import com.fantow.remoting.common.RemotingHelper;
import com.fantow.remoting.common.SemaphoreReleaseOnce;
import com.fantow.remoting.common.ServiceThread;
import com.fantow.remoting.exception.RemotingException;
import com.fantow.remoting.protocol.RPCHook;
import com.fantow.remoting.protocol.RemotingCommand;
import com.fantow.remoting.protocol.RemotingSysResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;

public abstract class AbstractNettyRemoting {

    private final static Logger logger = LoggerFactory.getLogger(AbstractNettyRemoting.class);

    public Semaphore semaphoneAsync;

    // 信号量可以限制正在进行的oneway请求的最大数量，从而保护系统内存
    public Semaphore semaphoreOneway;

    // 这个map会缓存所有正在进行的请求
    // <opaque,ResponseFuture>
    public ConcurrentHashMap<Integer,ResponseFuture> responseTable = new ConcurrentHashMap<>(256);

    // 这个map缓存所有requestCode以及其对应的RequestProcessor
    public HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<>(64);

    // 线程池
    public NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    // 默认的请求处理器
    public Pair<NettyRequestProcessor,ExecutorService> defaultRequestProcessor;

    // 为了支持SSL
    public volatile SslContext sslContext;

    // rpcHook的集合
    public List<RPCHook> rpcHooks = new ArrayList<>();

    // 为什么不需要限制Sync的请求量？因为不会用到阻塞队列？
    public AbstractNettyRemoting(int permitOneway,int permitAsync){
        this.semaphoreOneway = new Semaphore(permitOneway);
        this.semaphoneAsync = new Semaphore(permitAsync);
    }

    // 自定义事件监听器
    public abstract ChannelEventListener getChannelEventListener();

    public void putNettyEvent(NettyEvent event){
        this.nettyEventExecutor.putNettyEvent(event);
    }

    // 输入RemotingCommand进行处理
    // 这里的command应该是通道中接收的command
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand command){
        if(command != null) {
            switch (command.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, command);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, command);
                    break;
                default:
                    break;
            }
        }
    }





    // 处理远程对等方发出的传入请求命令。
    public void processRequestCommand(ChannelHandlerContext ctx,RemotingCommand cmd){
        Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        Pair<NettyRequestProcessor, ExecutorService> processorPair = matched == null ? this.defaultRequestProcessor : matched;

        int opaque = cmd.getOpaque();

        if(processorPair != null){
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        // 首先执行前置增强操作
                        doBeforeRpcHooks(RemotingHelper.parseChannelRemotingAddr(ctx.channel(), cmd));

                        // 设置回调函数
                        RemotingResponseCallback callback = new RemotingResponseCallback() {
                            @Override
                            public void callback(RemotingCommand response) {
                                doAfterRpcHooks(RemotingHelper.parseChannelRemotingAddr(ctx.channel()), cmd, response);

                                // 如果是oneway，不用响应结果
                                if (!cmd.isOnewayRpc()) {
                                    if (response != null) {
                                        // 为response添加opaque和置消息类型为response
                                        response.setOpaque(opaque);
                                        response.markResponseType();
                                        try {
                                            ctx.writeAndFlush(response);
                                        } catch (Throwable e) {
                                            logger.info("process request has failed: " + e);
                                        }
                                    } else {
                                        // 如果没有回复的消息，是否也应该回复一个确认包？
                                        // 和oneway方式的区别在于：oneway不保证一定可靠
                                        // 但是其他通信方式应该保证
                                    }
                                }
                            }
                        };
                        // processor的作用就是处理请求，并且返回response的RemotingCommand形式
                        // 是否对应的processor是专门用来处理异步通信的
                        if (processorPair.getObject1() instanceof AsyncNettyRequestProcessor) {
                            AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor) processorPair.getObject1();
                            processor.asyncProcessRequest(ctx, cmd, callback);
                        } else {
                            NettyRequestProcessor processor = processorPair.getObject1();
                            RemotingCommand response = processor.processRequest(ctx, cmd);
                            callback.callback(response);
                        }
                    }catch (Throwable e){
                        logger.info("Process request error: " + e);

                        // 即使是processor处理失败，也要返回一个处理失败的请求
                        if(!cmd.isOnewayRpc()){
                            RemotingCommand response = RemotingCommand.createResponseCommand(
                                RemotingSysResponseCode.SYSTEM_ERROR,RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            // 将任务封装在RequestTask中，并交给线程池执行
            RequestTask requestTask = new RequestTask(runnable,ctx.channel(),cmd);
            processorPair.getObject2().submit(requestTask);
        }else{
            // 既没有对应的Processor又没有默认的processor
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,"Can not find a matched processor");
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            logger.info("Can not find a matched Processor");
        }
    }

    // 处理相应请求
    public void processResponseCommand(ChannelHandlerContext ctx,RemotingCommand cmd){
        int opaque = cmd.getOpaque();

        // 向responseTable存入的操作在invoke操作中进行
        ResponseFuture responseFuture = responseTable.get(opaque);
        if(responseFuture != null){
            responseFuture.setResponseCommand(cmd);
            // 将responseFuture移除
            responseTable.remove(opaque);

            if(responseFuture.getInvokeCallback() != null){
                executeInvokeCallback(responseFuture);
            }else{
                responseFuture.putResponse(cmd);
//                responseFuture.release();
            }
        }else{
            logger.info("receive response,but can not match any request");
        }
    }


    private void executeInvokeCallback(ResponseFuture responseFuture){
        // 判断是否在当前线程执行
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();

        // 如果实现类中指定了自定义线程池
        if(executor != null){
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        responseFuture.executeInvokeCallback();
                    }
                });
            } catch(Exception ex){
                runInThisThread = true;
                logger.info("executor submit error,this job will execute in this Thread.");
            }
        }else{
            runInThisThread = true;
        }

        if(runInThisThread){
            responseFuture.executeInvokeCallback();
        }
    }

    // 获取Callback线程池
    public abstract ExecutorService getCallbackExecutor();


    // 定时扫描responseTable，并且剔除超时的responseTask
    public void scanResponseTable(){
        Iterator<Map.Entry<Integer, ResponseFuture>> iterator = this.responseTable.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<Integer, ResponseFuture> next = iterator.next();
            ResponseFuture responseFuture = next.getValue();
            // 判断是否超时
            if((responseFuture.getBeginTimestamp() + responseFuture.getTimeoutMillis())
                        > System.currentTimeMillis()){
                logger.info("remove the timeout request: " + responseFuture);
                iterator.remove();
            }
        }
    }

    // 进行同步调用
    // 发送请求
    public RemotingCommand invokeSyncImpl(Channel channel,RemotingCommand request,long timeoutMillis) throws InterruptedException, RemotingException {

        int opaque = request.getOpaque();

        try{
            // 看来每个request无论同步异步都要创建一个responseFuture
            ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null,null);
            this.responseTable.put(opaque,responseFuture);

            // 获取通道另一侧的socketAddress
            SocketAddress remoteAddress = channel.remoteAddress();

            // 发送请求，同时对通道添加监听器
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(future.isSuccess()){
                        responseFuture.setSendRequestOk(true);
                        return;
                    }else{
                        responseFuture.setSendRequestOk(false);

                        responseTable.remove(opaque);
                        responseFuture.putResponse(null);
                    }
                }
            });

            // 这里收到的responseCommand就是响应
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if(responseCommand == null){
                if(responseFuture.isSendRequestOK()){
                    throw new RemotingException("the response timeout.");
                }else{
                    throw new RemotingException("Remoting send request error.");
                }
            }

            return responseCommand;
        }finally {
            // 同步方式会阻塞直到收到response，所以走到这一步一定可以移除
            this.responseTable.remove(opaque);
        }
    }

    // 异步方式
    public void invokeAsyncImpl(Channel channel, RemotingCommand request, long timeoutMillis,
                                 InvokeCallback invokeCallback) throws RemotingException {

        long beginTime = System.currentTimeMillis();

        int opaque = request.getOpaque();
        // 因为会通过信号量机制限制异步发送请求
        // 这里首先尝试获取许可
        boolean acquired = false;
        try {
            acquired = this.semaphoneAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
            SemaphoreReleaseOnce once = new SemaphoreReleaseOnce(this.semaphoneAsync);

            long costTime = System.currentTimeMillis() - beginTime;

            // 未超时获取到
            if(acquired){
                ResponseFuture responseFuture = new ResponseFuture(channel,opaque,
                                                timeoutMillis - costTime,invokeCallback,once);
                this.responseTable.put(opaque,responseFuture);

                try {
                    channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                responseFuture.setSendRequestOk(true);
                                return;
                            } else {
                                // request发送失败
                                responseFuture.setSendRequestOk(false);
                                ResponseFuture responseFuture1 = responseTable.remove(opaque);
                                // 释放信号量
                                responseFuture1.release();
                            }
                        }
                    });
                }catch (Exception ex){
                    responseFuture.release();
                }
            }else{
                // 超时未获取到
                once.release();
                throw new RemotingException("InvokeAsyncImpl call timeout.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 将指定通道的请求标记为失败并立即调用失败回调
    // 应该是要用于：如果检测到这个Channel不可用，立即删除所有使用该通道的responseFuture
    protected void fastFailed(Channel channel){
        Iterator<Map.Entry<Integer, ResponseFuture>> iterator = this.responseTable.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<Integer, ResponseFuture> entry = iterator.next();
            if(entry.getValue().getProcessChannel() == channel){
                Integer opaque = entry.getKey();
                if(opaque != null){
                    ResponseFuture future = responseTable.remove(opaque);
                    future.release();
                }
            }

        }
    }


    public void invokeOnewayImpl(Channel channel,RemotingCommand request,long timeoutMillis) throws InterruptedException, RemotingException {

        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis,TimeUnit.MILLISECONDS);

        if(acquired){
            final SemaphoreReleaseOnce once = new SemaphoreReleaseOnce(this.semaphoreOneway);
            try{
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        once.release();
                        if(!future.isSuccess()){
                            logger.info("Send the request fail.");
                            throw new RemotingException("Send the request fail.");
                        }
                    }
                });
            }catch(Exception ex){
                logger.info("Send the request fail.");
                throw new RemotingException("Send the request fail.");
            }
        }else{
            logger.info("Get the semaphore timeout");
            throw new RemotingException("Get the semaphore timeout");
        }
    }

    
    public void doBeforeRpcHooks(String addr,RemotingCommand request){
        if(rpcHooks.size() > 0){
            for(RPCHook rpcHook : rpcHooks){
                rpcHook.doBeforeRequest(addr,request);
            }
        }
    }

    public void doAfterRpcHooks(String addr,RemotingCommand request,RemotingCommand response){
        if(rpcHooks.size() > 0){
            for(RPCHook rpcHook : rpcHooks){
                rpcHook.doAfterRequest(addr, request, response);
            }
        }
    }

    class NettyEventExecutor extends ServiceThread {
        // 一个阻塞队列，用于存放NettyEvent
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        // 将NettyEvent存入阻塞队列
        public void putNettyEvent(NettyEvent event){
            if(this.eventQueue.size() <= maxSize){
                this.eventQueue.add(event);
            }else{
                logger.info("the Netty Event Queue is Full");
            }
        }

        @Override
        public void run() {
            logger.info(this.getServiceName() + " service is started");

            // 获取事件监听器
            ChannelEventListener listener = AbstractNettyRemoting.this.getChannelEventListener();

            // 循环监听事件
            while(!this.isStopped()){
                try{
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if(event != null && listener != null){
                        switch(event.getType()){
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(),event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(),event.getChannel());
                                break;
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(),event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(),event.getChannel());
                                break;
                            default:
                                break;
                        }
                    }
                }catch(Exception ex){
                    logger.info(this.getServiceName() + " service has exception. " + ex);
                }
            }

            logger.info(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }





}
