package com.hazelcast.client.connection.nio;

import com.hazelcast.client.impl.ClientTypes;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.nio.IOUtil.compactOrClear;

public class ClientAuthenticationRequestEncoder extends OutboundHandler<ByteBuffer, ByteBuffer> {

    private final InternalSerializationService serializationService;
    private final ICredentialsFactory credentialsFactory;
    private final ClientPrincipal principal;
    private final boolean asOwner = true;
    private ClientMessage authRequest;

    public ClientAuthenticationRequestEncoder(InternalSerializationService serializationService,
                                              ICredentialsFactory credentialsFactory,
                                              ClientPrincipal principal) {
        this.serializationService = serializationService;
        this.credentialsFactory = credentialsFactory;
        this.principal = principal;
    }

    @Override
    public void handlerAdded() {
        authRequest = encodeAuthenticationRequest();
        authRequest.setCorrelationId(39373);
        authRequest.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);
        System.out.println(channel+" Authentication:"+authRequest.getFrameLength()+" bytes");
    }

    @Override
    public HandlerStatus onWrite() {
        //System.out.println(channel+" ClientAuthenticationRequestEncoder.onWrite "+ IOUtil.toDebugString("dst",dst));
        compactOrClear(dst);
        try{
            if (authRequest.writeTo(dst)) {
                // authentication request is written, so lets remove ourselves from the pipeline
                channel.outboundPipeline().remove(this);
                System.out.println(channel+" Removing ClientAuthenticationRequestEncoder from pipeline");
                return CLEAN;
            } else {
                // the message didn't get written completely, so we are done.
                return DIRTY;
            }
        }finally {
            dst.flip();
        }
    }

    private ClientMessage encodeAuthenticationRequest() {
        byte serializationVersion = serializationService.getVersion();
        String uuid = null;
        String ownerUuid = null;
        if (principal != null) {
            uuid = principal.getUuid();
            ownerUuid = principal.getOwnerUuid();
        }
        Credentials credentials = credentialsFactory.newCredentials();
        //lastCredentials = credentials;
        if (credentials.getClass().equals(UsernamePasswordCredentials.class)) {
            UsernamePasswordCredentials cr = (UsernamePasswordCredentials) credentials;
            return ClientAuthenticationCodec.encodeRequest(
                    "fffffffffffffffffffffffffffffffffffffffffffffffeeefefeff",
                    cr.getPassword(),
                    uuid,
                    ownerUuid,
                    asOwner,
                    ClientTypes.JAVA,
                    serializationVersion,
                    BuildInfoProvider.getBuildInfo().getVersion());
        } else {
            return ClientAuthenticationCustomCodec.encodeRequest(
                    serializationService.toData(credentials),
                    uuid,
                    ownerUuid,
                    asOwner,
                    ClientTypes.JAVA,
                    serializationVersion,
                    BuildInfoProvider.getBuildInfo().getVersion());
        }
    }
}
