package com.fantow.remoting.netty;

import com.fantow.remoting.ChannelEventListener;
import com.fantow.remoting.InvokeCallback;
import com.fantow.remoting.RemotingServer;
import com.fantow.remoting.common.Pair;
import com.fantow.remoting.common.RemotingUtil;
import com.fantow.remoting.common.TlsMode;
import com.fantow.remoting.exception.RemotingException;
import com.fantow.remoting.protocol.RPCHook;
import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class NettyRemotingServer extends AbstractNettyRemoting implements RemotingServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyRemotingServer.class);

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupSelector;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;

    private final ExecutorService publicExecutor;
    private final ChannelEventListener channelEventListener;

    private final Timer timer = new Timer("ServerHouseKeepingService",true);
    // Netty中提供的默认线程池
    private DefaultEventExecutorGroup defaultEventExecutorGroup;


    private int port = 0;

    private static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    private static final String TLS_HANDLER_NAME = "sslHandler";
    private static final String FILE_REGION_ENCODER_NAME = "fileRegionEncoder";

    // sharable handler
    private HandshakeHandler handshakeHandler;
    private NettyEncoder encoder;
    private NettyConnectManageHandler connectManageHandler;
    private NettyServerHandler serverHandler;


    public NettyRemotingServer(NettyServerConfig nettyServerConfig){
        this(nettyServerConfig,null);
    }

    // 传入参数为ServerConfig和eventListener
    public NettyRemotingServer(NettyServerConfig nettyServerConfig,
                               ChannelEventListener channelEventListener){
        super(nettyServerConfig.getServerOnewaySemaphoreValue(),nettyServerConfig.getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        // 配置执行callback的线程池大小
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if(publicThreadNums <= 0){
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {

            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,String.format("NettyServerPublicExecutor_%d",this.threadIndex.incrementAndGet()));
            }
        });

        if(useEpoll()){
            // 配置Boss
            this.eventLoopGroupBoss = new EpollEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r,String.format("NettyEpollBoss_%d",this.threadIndex.incrementAndGet()));
                }
            });

            // 这个应该就是Worker
            this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r,String.format("NettyEpollWorker_%d",threadIndex.incrementAndGet()));
                }
            });
        }else{
            // 正常使用Netty
            this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r,String.format("NettyNioBoss_%d",threadIndex.incrementAndGet()));
                }
            });


            this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerWorkerThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r,String.format("NettyNioWorker_%d",threadIndex.incrementAndGet()));
                }
            });


        }

//        loadSSLContext();


    }


//    public void loadSSLContext(){
//        // 默认为permission
//        TlsMode tlsMode = TlsSystemConfig.tlsMode;
//
//        if(tlsMode != TlsMode.DISABLE){
//            sslContext
//
//        }
//
//    }




    private boolean useEpoll(){
        return RemotingUtil.isLinuxPlatform()
                && nettyServerConfig.isUseEpollNativeSelector()
                && Epoll.isAvailable();
    }


    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyServerConfig.getServerWorkerThreads(), new ThreadFactory() {

            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"NettyServerCodeThread_" + this.threadIndex.incrementAndGet());
            }
        });

        prepareSharableHandlers();


        this.serverBootstrap.group(this.eventLoopGroupBoss,this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG,1024)
                .option(ChannelOption.SO_REUSEADDR,true)
                .option(ChannelOption.SO_KEEPALIVE,false)
                .childOption(ChannelOption.TCP_NODELAY,true)
                .childOption(ChannelOption.SO_SNDBUF,nettyServerConfig.getServerSocketSendBufSize())
                .childOption(ChannelOption.SO_RCVBUF,nettyServerConfig.getServerSocketRecvBufSize())
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(defaultEventExecutorGroup,HANDSHAKE_HANDLER_NAME,handshakeHandler)
                                .addLast(defaultEventExecutorGroup,encoder,
                                        new NettyDecoder(),
                                        new IdleStateHandler(0,0,nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                        connectManageHandler,
                                        serverHandler
                                        );
                    }
                });

        // 是否为ByteBuf开启缓冲池
        if(nettyServerConfig.isServerPooledByteBufferAllocatorEnable()){
            this.serverBootstrap.childOption(ChannelOption.ALLOCATOR,
                    PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture channelFuture = this.serverBootstrap.bind().sync();
        } catch (InterruptedException e) {
            logger.error("ServerBootstrap bind error, {}",e);
        }

        // 扫描并剔除过期任务
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                NettyRemotingServer.this.scanResponseTable();
            }
        },1000 * 3,1000);

    }


    @Override
    public void shutdown() {

        if(this.timer != null){
            this.timer.cancel();
        }

        this.eventLoopGroupBoss.shutdownGracefully();

        this.eventLoopGroupSelector.shutdownGracefully();

        if(this.nettyEventExecutor != null){
            this.nettyEventExecutor.shutdown();
        }

        if(this.defaultEventExecutorGroup != null){
            this.defaultEventExecutorGroup.shutdownGracefully();
        }

        if(this.publicExecutor != null){
            publicExecutor.shutdown();
        }

    }








    public void prepareSharableHandlers(){
        this.handshakeHandler = new HandshakeHandler(TlsSystemConfig.tlsMode);
        this.encoder = new NettyEncoder();
        this.connectManageHandler = new NettyConnectManageHandler();
        this.serverHandler = new NettyServerHandler();
    }


    @ChannelHandler.Sharable
    class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf>{

        // 设置SSL类型(禁用，强制，许可)
        private final TlsMode tlsMode;

        private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

        HandshakeHandler(TlsMode tlsMode){
            this.tlsMode = tlsMode;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            // 标记当前位置，以便我们可以窥视第一个字节以确定内容是否以TLS握手
            msg.markReaderIndex();
            // 获取msg中的一个字，判断HandShake类型
            byte b = msg.getByte(0);

            if(b == HANDSHAKE_MAGIC_CODE){
                switch (tlsMode){
                    // 客户端向要建立SSL连接，但是服务器端禁用SSL
                    case DISABLE:
                        // 关闭
                        ctx.close();
                        logger.warn("client wants to establish an SSL connection but this server is running in SSL disable mode.");
                        break;
                    case PERMISSIVE:
                    case ENFORCING:
                        if(sslContext != null){
                            // 这部分应该是对pipeline中加Handler
                            // 但是这个addAfter应该是加在哪里？
                            // 感觉应该是按顺序加的
                            ctx.pipeline().addAfter(defaultEventExecutorGroup,HANDSHAKE_HANDLER_NAME,TLS_HANDLER_NAME,
                                            sslContext.newHandler(ctx.channel().alloc()))
                                        .addAfter(defaultEventExecutorGroup,TLS_HANDLER_NAME,FILE_REGION_ENCODER_NAME,new FileRegionEncoder());
                            logger.info("Handlers prepended to channel pipeline to establish SSL connection");
                        }else{
                            ctx.close();
                            logger.error("sslContext is null");
                        }
                        break;
                    default:
                        logger.info("Unknown TLS mode");
                }
            }else if(tlsMode == TlsMode.ENFORCING){
                // 如果msg中的第一个字没有作为魔数出现，说明client没有建立SSL连接
                ctx.close();
                logger.info("client wants to establish an insecure connection while this server is running in enforcing mode");
            }

            // 为msg复位
            msg.resetReaderIndex();

            try{
                ctx.pipeline().remove(this);
            }catch (NoSuchElementException e){
                logger.error("something wrong while removing HandShakeHandler: ",e);
            }
            // 传递消息至下一个处理器
            ctx.fireChannelRead(msg.retain());
        }
    }

    @ChannelHandler.Sharable
    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand>{

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx,msg);
        }
    }

    // 连通后的处理逻辑
    @ChannelHandler.Sharable
    class NettyConnectManageHandler extends ChannelDuplexHandler{

        // Channel已经被创建，并且已经注册到EventLoop上
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("Netty server register:{}",remoteAddress);
            super.channelRegistered(ctx);
        }

        // Channel被从EventLoop中被注销
        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("Netty server unregister:{}",remoteAddress);
            super.channelUnregistered(ctx);
        }

        // Channel处于活动状态(已经连接到它的远程结点)，并且可以接受/发送数据了
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("Netty server active:{}",remoteAddr);
            super.channelActive(ctx);

            // 一旦连接，就将连接事件加入到eventQueue中
            if(NettyRemotingServer.this.channelEventListener != null){
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT,remoteAddr,ctx.channel()));
            }
        }

        // Channel没有连接到远程结点
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            logger.info("Netty Server inactive:{}",remoteAddress);
            super.channelInactive(ctx);

            if(NettyRemotingServer.this.channelEventListener != null){
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE,remoteAddress,ctx.channel()));
            }
        }

        // 当ChannelInboundHandler.fireUserEventTriggered()方法被调用时调用该方法
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            super.userEventTriggered(ctx, evt);
        }

        // 当处理过程中，在ChannelPipeline中有错误产生时被调用
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }


    // 注册Processor
    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorService = executor;
        if(executor == null){
            executorService = this.publicExecutor;
        }
        Pair<NettyRequestProcessor,ExecutorService> pair = new Pair<>(processor,executorService);
        this.processorTable.put(requestCode,pair);
    }

    // 注册默认Processor
    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<>(processor,executor);
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return this.processorTable.get(requestCode);
    }

    @Override
    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) throws RemotingException, InterruptedException {
        return invokeSyncImpl(channel,request,timeoutMillis);
    }

    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws RemotingException {
        invokeAsyncImpl(channel,request,timeoutMillis,invokeCallback);
    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand remotingCommand, long timeoutMillis) throws RemotingException, InterruptedException {
        invokeOnewayImpl(channel,remotingCommand,timeoutMillis);
    }

    // 添加RPCHook
    @Override
    public void registerRPCHook(RPCHook rpcHook) {
        if(rpcHook != null && !rpcHooks.contains(rpcHook)){
            rpcHooks.add(rpcHook);
        }
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return this.channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }
}
