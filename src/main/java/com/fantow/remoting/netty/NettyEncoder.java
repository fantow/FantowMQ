package com.fantow.remoting.netty;

import com.fantow.remoting.common.RemotingUtil;
import com.fantow.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

// Message转Encoder
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {

    private static final Logger logger = LoggerFactory.getLogger(NettyEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) throws Exception {

        try {
            ByteBuffer header = remotingCommand.encodeHeader();
            out.writeBytes(header);

            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch(Exception ex){
            logger.info("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()),ex);
            if(remotingCommand != null){
                logger.info(remotingCommand.toString());
            }
            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
