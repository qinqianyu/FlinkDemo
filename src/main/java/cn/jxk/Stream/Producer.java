package cn.jxk.Stream;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Producer {
    public static void main(String[] args) {
        connect("127.0.0.1", 9000);
    }
    public static void connect(String host, int port) {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            Socket accept = serverSocket.accept();
            OutputStream outputStream = accept.getOutputStream();
            PrintStream printStream = new PrintStream(outputStream);
            for (int i = 0; i < 20; i++) {
                Thread.sleep(10000);
                printStream.println("char--" + i);
            }
            printStream.println();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
