package rpckids.server;

public class RPCServerStarter {

	public static void main(String[] args) {
		RPCServer server = new RPCServer("localhost", 8888, 2, 16);
		server.start();
	}
	
}
