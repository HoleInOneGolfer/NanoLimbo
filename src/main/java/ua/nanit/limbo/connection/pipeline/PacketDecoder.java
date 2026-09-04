package ua.nanit.limbo.connection.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

import ua.nanit.limbo.connection.ClientConnection;
import ua.nanit.limbo.protocol.ByteMessage;
import ua.nanit.limbo.protocol.PacketIn;
import ua.nanit.limbo.protocol.registry.State;

public class PacketDecoder extends ByteToMessageDecoder {

    private final ClientConnection connection;

    public PacketDecoder(ClientConnection connection) {
        this.connection = connection;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (!in.isReadable()) {
            return;
        }

        int mark = in.readerIndex();
        try {
            // NanoLimbo uses ByteMessage to read VarInts
            int packetId = ByteMessage.readVarInt(in);
            State state = this.connection.getState();

            // Intercept Custom Payloads / Plugin Messages during CONFIGURATION or LOGIN phase
            if (state == State.CONFIGURATION || state == State.LOGIN) {
                if (packetId == 0x00) {
                    // Silently consume the custom payload byte buffer
                    in.skipBytes(in.readableBytes());
                    return;
                }
            }

            // Normal packet decoding
            PacketIn packet = state.getPacketIn(this.connection.getProtocolVersion(), packetId);
            if (packet != null) {
                packet.read(in, this.connection.getProtocolVersion());
                out.add(packet);
            } else {
                in.skipBytes(in.readableBytes());
            }

        } catch (Exception e) {
            // Catch decoding errors during handshake phase to prevent Netty channel closure
            in.readerIndex(in.writerIndex());
        }
    }
}