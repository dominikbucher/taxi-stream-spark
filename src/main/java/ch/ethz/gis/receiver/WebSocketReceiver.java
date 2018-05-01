package ch.ethz.gis.receiver;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

public class WebSocketReceiver extends Receiver<String> implements Runnable {
    private String url;

    private Thread thread = null;
    private WebSocketClient client = null;

    WebSocketReceiver(StorageLevel storageLevel, String url) {
        super(storageLevel);

        this.url = url;
    }

    public void onStart() {
        try {
            client = new EmptyClient(new URI(url));
            thread = new Thread(this);
            thread.start();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    public void onStop() {
        client.close();
        thread.interrupt();
    }

    public void run() {
        client.connect();
    }

    public class EmptyClient extends WebSocketClient {
        EmptyClient(URI serverURI) {
            super(serverURI);
        }

        @Override
        public void onOpen(ServerHandshake handshakedata) {
            System.out.println("Opened new WebSocket channel.");
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            System.out.println("Opened WebSocket channel with exit code " + code + " additional info: " + reason);
        }

        @Override
        public void onMessage(String message) {
            store(message);
        }

        @Override
        public void onMessage(ByteBuffer message) {
        }

        @Override
        public void onError(Exception ex) {
            System.out.println("Error while receiving message: " + ex.getMessage());
        }
    }
}