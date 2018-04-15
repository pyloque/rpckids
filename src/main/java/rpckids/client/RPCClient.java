package rpckids.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import com.alibaba.fastjson.JSON;

import rpckids.common.Charsets;

public class RPCClient {

	private String ip;
	private int port;
	private Socket sock;
	private DataInputStream input;
	private OutputStream output;

	public RPCClient(String ip, int port) {
		this.ip = ip;
		this.port = port;
	}

	public void connect() throws IOException {
		SocketAddress addr = new InetSocketAddress(ip, port);
		sock = new Socket();
		sock.connect(addr, 5000);
		input = new DataInputStream(sock.getInputStream());
		output = sock.getOutputStream();
	}

	public void close() {
		try {
			sock.close();
			sock = null;
			input = null;
			output = null;
		} catch (IOException e) {
		}
	}

	public Object send(String type, Object payload) {
		try {
			return this.sendInternal(type, payload, false);
		} catch (IOException e) {
			throw new RPCException(e);
		}
	}

	public RPCClient rpc(String type, Class<?> clazz) {
		ResponseRegistry.register(type, clazz);
		return this;
	}

	public void cast(String type, Object payload) {
		try {
			this.sendInternal(type, payload, true);
		} catch (IOException e) {
			throw new RPCException(e);
		}
	}

	private Object sendInternal(String type, Object payload, boolean cast) throws IOException {
		if (output == null) {
			connect();
		}
		String requestId = RequestId.next();
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream buf = new DataOutputStream(bytes);
		writeStr(buf, requestId);
		writeStr(buf, type);
		writeStr(buf, JSON.toJSONString(payload));
		buf.flush();
		byte[] fullLoad = bytes.toByteArray();
		try {
			output.write(fullLoad);
		} catch (IOException e) {
			close();
			connect();
			output.write(fullLoad);
		}
		if (!cast) {
			String reqId = readStr();
			if (!requestId.equals(reqId)) {
				close();
				throw new RPCException("request id mismatch");
			}
			String typ = readStr();
			Class<?> clazz = ResponseRegistry.get(typ);
			if (clazz == null) {
				throw new RPCException("unrecognized rpc response type=" + typ);
			}
			String payld = readStr();
			Object res = JSON.parseObject(payld, clazz);
			return res;
		}
		return null;
	}

	private String readStr() throws IOException {
		int len = input.readInt();
		byte[] bytes = new byte[len];
		input.readFully(bytes);
		return new String(bytes, Charsets.UTF8);
	}

	private void writeStr(DataOutputStream out, String s) throws IOException {
		out.writeInt(s.length());
		out.write(s.getBytes(Charsets.UTF8));
	}
}
