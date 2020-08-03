package com.fantow.remoting.netty;

import com.fantow.remoting.ChannelEventListener;
import com.fantow.remoting.InvokeCallback;
import com.fantow.remoting.RemotingClient;
import com.fantow.remoting.common.Pair;
import com.fantow.remoting.common.RemotingHelper;
import com.fantow.remoting.common.RemotingUtil;
import com.fantow.remoting.exception.RemotingException;
import com.fantow.remoting.protocol.RPCHook;
import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NettyRemotingClient extends AbstractNettyRemoting implements RemotingClient {

    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingClient.class);
    // ???
    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final Lock lockChannelTables = new ReentrantLock();
//    Map<address,ChannelWrapper>
    private final ConcurrentHashMap<String,ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();

    private final Timer timer = new Timer("ClientHouseKeepingService",true);

    // client中用来记录namesrv地址的list
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();

    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<>();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final Lock lockNamesrvChannel = new ReentrantLock();

    private final ExecutorService publicExecutor;

    private ExecutorService callbackExecutor;
    private final ChannelEventListener channelEventListener;
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    public NettyRemotingClient(NettyClientConfig nettyClientConfig){
        this(nettyClientConfig,null);
    }

    public NettyRemotingClient(NettyClientConfig nettyClientConfig,
                               ChannelEventListener channelEventListener){
        super(nettyClientConfig.getClientOnewaySemaphoreValue(),
                nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if(publicThreadNums <= 0){
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"NettyCLientPublicExecutor_" + threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"NettyClientWorker_" + threadIndex.incrementAndGet());
            }
        });

        // SSL支持部分没写

    }

    private static int initValueIndex(){
        Random r = new Random();
        return Math.abs(r.nextInt() % 999) % 999;
    }

    // 现在还没明白这个的作用
    static class ChannelWrapper{
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture){
            this.channelFuture = channelFuture;
        }

        // 判断channelFuture对应的channel是否active
        public boolean isAlive(){
            return this.channelFuture != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable(){
            return this.channelFuture.channel().isWritable();
        }

        public Channel getChannel(){
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture(){
            return this.channelFuture;
        }

    }


    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if(!addrs.isEmpty()){
            if(old == null){
                update = true;
            }else if(addrs.size() != old.size()){
                update = true;
            }else{
                for(int i = 0;i < addrs.size();i++){
                    if(!old.contains(addrs.get(i))){
                        update = true;
                    }
                }
            }

            if(update){
                Collections.shuffle(addrs);
                this.namesrvAddrList.set(addrs);

                if(!addrs.contains(this.namesrvAddrChoosed.get())){
                    this.namesrvAddrChoosed.set(null);
                }
            }
        }

    }

    @Override
    public List<String> getNameServerAddressList() {
        return null;
    }

    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingException {
        long beginStartTime = System.currentTimeMillis();
        Channel channel = this.getAndCreateChannel(addr);
        if(channel != null && channel.isActive()){
            // 现在还不知道这个doBeforeRpcHooks和doAfterRpcHooks为什么会出现在invokeSync中？？？
            doBeforeRpcHooks(addr,request);
            RemotingCommand response = this.invokeSyncImpl(channel,request,timeoutMillis);

            doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(channel),request,response);
            return response;
        }else{
            this.closeChannel(channel);
            throw new RemotingException("can not get a right channel about remoting: " + addr);
        }

    }

    // 参数addr是指NameSrv的addr
    // 如果给定addr不为空，要么去channelTable中去找，要么就主动连接一次
    // 如果给定addr为空，就重新从NameSrvList中拿一个addr
    private Channel getAndCreateChannel(String addr) throws InterruptedException {
        if(addr == null){
            return getAndCreateNameServerChannel();
        }

        // ChannelWrapper中封装的是channelFuture
        ChannelWrapper cw = this.channelTables.get(addr);
        if(cw != null && cw.isAlive()){
            return cw.getChannel();
        }

        return this.createChannel(addr);
    }

    private Channel getAndCreateNameServerChannel() throws InterruptedException {
        // 获取到之前选定的namesrv的addr
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isAlive()) {
                return cw.getChannel();
            }
        }

        List<String> addrList = this.namesrvAddrList.get();
        if (this.lockNamesrvChannel.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                // 保证即使是不同线程进入这部分，获取到的channel都是同一份。
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isAlive()) {
                        return cw.getChannel();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index) % addrList.size();
                        String newAddr = addrList.get(index);

                        this.namesrvAddrChoosed.set(newAddr);
                        logger.info("The new NameServer is chosen: " + newAddr);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                }
            } finally {
                this.lockNamesrvChannel.unlock();
            }

            return null;
        }
    }

    // 根据Addr从channelTables中获取Channel
    // 如果无法从channelTables中获取，会进行连接，并将channel包装在ChannelWrapper中
    // 再存入channelTables中。
    private Channel createChannel(String addr) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if(cw != null && cw.isAlive()){
            return cw.getChannel();
        }

        if(this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS,TimeUnit.MILLISECONDS)){

            try {
                boolean createNewConnection;
                cw = this.channelTables.get(addr);
                // 为什么要分这么多的if分支，可能是因为Channel创建比较耗时
                if (cw != null) {
                    if (cw.isAlive()) {
                        return cw.getChannel();
                    } else if (!cw.channelFuture.isDone()) {
                        // 表示创建还未结束，但是正在创建
                        createNewConnection = false;
                    } else {
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                // 如果的确需要创建新的Channel
                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    logger.info("call the method createChannel to create new Channel for addr:{}", addr);

                    cw = new ChannelWrapper(channelFuture);
                    // 其实将cw存入channelTables的逻辑应该在下面验证完cw.isAlive()中进行
                    // 但是使用时每次都会在获取cw时判断.isAlive()，所以无所谓了。
                    this.channelTables.put(addr, cw);
                }
            }finally {
                this.lockChannelTables.unlock();
            }
        }else{
            logger.error("Can not get ChannelTable until timeout.");
        }

        if(cw != null){
            ChannelFuture channelFuture = cw.getChannelFuture();
            // 等待channelFuture完成直到超时
            if(channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectionTimeoutMillis())){
                if(cw.isAlive()){
                    logger.info("createChannel: connect remote host:{} success",addr);
                }else{
                    logger.info("createChannel: connect remote host:{} failed",addr);
                }
            }else{
                logger.info("createChannel:connect remote host:{} timeout",addr);
            }
        }

        return null;
    }


    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timoutMiilis, InvokeCallback invokeCallback) throws InterruptedException, RemotingException {

        Channel channel = this.getAndCreateChannel(addr);
        if(channel != null && channel.isActive()){
            try{
                doBeforeRpcHooks(addr,request);
                this.invokeAsyncImpl(channel,request,timoutMiilis,invokeCallback);
            }catch (Exception ex){
                logger.error("invokeAsync cause error:{}",ex);
                this.closeChannel(channel);
            }
        }else{
            this.closeChannel(channel);
            throw new RemotingException("Can not get a right channel about:" + addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingException {

        Channel channel = this.getAndCreateChannel(addr);
        if(channel != null && channel.isActive()){
            doBeforeRpcHooks(addr,request);
            this.invokeOnewayImpl(channel,request,timeoutMillis);

        }else{
            this.closeChannel(channel);
            throw new RemotingException("Can not get a right channel about: " + addr);
        }
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if(executorThis != null){
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor,ExecutorService> pair = new Pair<>(processor,executorThis);
        this.processorTable.put(requestCode,pair);
    }

    @Override
    public void setCallbackExecutor(ExecutorService callbackExecutor) {

    }

    @Override
    public void start() {
        // 初始化工作线程组
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(nettyClientConfig.getClientWorkerThread(),
                new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r,"NettyClientWorkerThread_" + threadIndex.incrementAndGet());
                    }
                });

        this.bootstrap.group(eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY,true)
                .option(ChannelOption.SO_KEEPALIVE,false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,nettyClientConfig.getConnectionTimeoutMillis())
                .option(ChannelOption.SO_SNDBUF,nettyClientConfig.getClientSocketSendBufSize())
                .option(ChannelOption.SO_RCVBUF,nettyClientConfig.getClientSocketRecvBufSize())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        if(nettyClientConfig.isUseTLS()){
                            if(sslContext != null){
                                pipeline.addFirst(defaultEventExecutorGroup,"sslHandler",sslContext.newHandler(ch.alloc()));
                                logger.info("Prepend SSL Handler");
                            }else{
                                logger.info("sslContext is null");
                            }
                        }

                        pipeline.addLast(defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0,0,nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                                new NettyConnectManageHandler(),
                                new NettyClientHandler()
                        );
                    }
                });

                this.timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        NettyRemotingClient.this.scanResponseTable();
                    }
                },1000 * 3,1000);

                if(this.channelEventListener != null){
                    this.nettyEventExecutor.start();
                }
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void registerRPCHook(RPCHook rpcHook) {

    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return null;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return null;
    }


    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand>{

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx,msg);
        }
    }




    // 主要是打Log以及将消息传入EventListener中
    class NettyConnectManageHandler extends ChannelDuplexHandler{
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
            // 本地/Client端的地址
            String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            // 远程端/Server端的地址
            String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            logger.info("Netty Client Pipeline: Connection {} => {}",local,remote);

            super.connect(ctx,remoteAddress,localAddress,promise);
            if(NettyRemotingClient.this.channelEventListener != null){
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT,remote,ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("Netty Client Pipeline: Disconnection {}",remoteAddr);
            closeChannel(ctx.channel());
            super.disconnect(ctx,promise);

            if(NettyRemotingClient.this.channelEventListener != null){
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE,remoteAddr,ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("Netty Client Pipeline: Close {}",remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx,promise);
            // fastFailed()逻辑就是从responseTable中找出使用相关channel的responseFuture
            // 并将其移除，就不比占用responseFuture等待返回response了。
            NettyRemotingClient.this.fastFailed(ctx.channel());
            if(NettyRemotingClient.this.channelEventListener != null){
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE,remoteAddress,ctx.channel()));
            }
        }

        // 用来支持心跳检测
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if(evt instanceof IdleStateEvent){
                IdleStateEvent event = (IdleStateEvent) evt;
                if(event.state().equals(IdleState.ALL_IDLE)){
                    String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    logger.info("Netty Client Pipeline: IDLE exception {}",remoteAddress);
                    closeChannel(ctx.channel());

                    if(NettyRemotingClient.this.channelEventListener != null){
                        NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE,remoteAddress,ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("Netty Client Pipeline: exceptionCaught {}",cause);
            closeChannel(ctx.channel());
            if(NettyRemotingClient.this.channelEventListener != null){
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION,remoteAddress,ctx.channel()));
            }
        }
    }

    public void closeChannel(Channel channel){
        if(channel == null){
            return;
        }

        try{
            if(this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS,TimeUnit.MILLISECONDS)){

                boolean removeItemFromTable = true;
                ChannelWrapper prevCW = null;
                String addrRemote = null;
                for(Map.Entry<String,ChannelWrapper> entry : channelTables.entrySet()){
                    String key = entry.getKey();
                    ChannelWrapper value = entry.getValue();
                    if(value != null && value.getChannel() != null){
                        if(value.getChannel() == channel){
                            addrRemote = key;
                            prevCW = value;
                            break;
                        }
                    }
                }

                // prevCW为null的可能是
                // 1.被其他线程删掉 --> 存在key
                // 2.本来就没有  --> 不存在key
                if(prevCW == null && addrRemote == null){
                    logger.info("eventCloseChannel:the channel has been remove");
                    removeItemFromTable = false;
                }

                if(removeItemFromTable){
                    this.channelTables.remove(addrRemote);
                    logger.info("closeChannel:the channel has been removed");
                    RemotingUtil.closeChannel(channel);
                }
            }
            this.lockChannelTables.unlock();
        }catch(Exception ex){
            logger.error("closeChannel() exception",ex);
        }

    }



}
