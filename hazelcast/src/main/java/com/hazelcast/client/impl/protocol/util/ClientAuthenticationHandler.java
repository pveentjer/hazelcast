package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.tcp.TcpIpConnection;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.nio.IOUtil.compactOrClear;

/**
 * Responsible for authenticating the client.
 *
 * This handler will remove itself from the pipeline as soon as the authentication
 * was a success. If the authentication has failed, it will not remove itself from
 * the pipeline and the handlers after this handler will not be triggered. So even
 * if there would be a malignant client, it can't execute invocations because
 * the pipeline is blocked.
 */
public class ClientAuthenticationHandler extends InboundHandler<ByteBuffer, Void> {

    private final ClientMessage authRequest = ClientMessage.create();
    private final Node node;

    public ClientAuthenticationHandler(Node node) {
         this.node = node;
    }


    @Override
    public HandlerStatus onRead() throws Exception {
        src.flip();
        try {
            if (!authRequest.readFrom(src)) {
                // authentication request has not yet been fully read
                return CLEAN;
            }

            Connection connection = (Connection) channel.attributeMap().get(TcpIpConnection.class);
            System.out.println(connection);
            AuthenticationMessageTask task = new AuthenticationMessageTask(authRequest, node, connection);
            task.run();

            System.out.println(channel+" fake processing of message"+authRequest+" task:"+task);
            channel.inboundPipeline().remove(this);
            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }
}
