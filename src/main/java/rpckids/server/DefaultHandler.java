package rpckids.server;

import io.netty.channel.ChannelHandlerContext;

public class DefaultHandler implements IMessageHandler<MessageInput> {

	@Override
	public void handle(ChannelHandlerContext ctx, String requesetId, MessageInput input) {
		System.out.println("unrecognized message type=" + input.getType() + " comes");
	}

}
