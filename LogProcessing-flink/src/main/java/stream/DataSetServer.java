package stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/* Configura um socket que transmite os dados de um arquivo de log para a aplicação processar. Caminho do arquivo é recebido como argumento da linha de comando.*/
public final class DataSetServer {

    /**
     * Configura porta do socket
     */
    public final static int SOCKET_PORT = 9999;


    public static void main(String[] args)
            throws Exception {

        final String filename;
        try {
            // acessa os argumentos da linha de comando
            if (args.length < 1) {
                System.err.println("No filename specified. Please run 'DataSetServer " +
                        "<filename>, where filename is the name of the dataset in csv format");
                return;
            } else {
                filename = args[0];
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'DataSetServer " +
                    "<filename>, where filename is the name of the dataset in csv format");
            return;
        }

        ServerSocket servsock = null;

        try {
            servsock = new ServerSocket(SOCKET_PORT);
            while (true) {
                System.out.println("Waiting...");

                Socket sock = null;

                try {
                    // recebe conexão
                    sock = servsock.accept();
                    System.out.println("Accepted connection : " + sock);

                    // acessa stream de saída
                    final OutputStream os = sock.getOutputStream();

                    // envia arquivo
                    final File myFile = new File(filename);
                    BufferedReader br = new BufferedReader(new FileReader(myFile));
                    try {
                        String line = br.readLine();

                        line = br.readLine();

                        while (line != null) {
                            os.write(line.getBytes("US-ASCII"));
                            os.write("\n".getBytes("US-ASCII"));
                            os.flush();
                            System.out.println(line);

                            // produz um atraso para processamento pela aplicação
                            final String minutes = line.substring(line.length() - 3);
                            TimeUnit.MILLISECONDS.sleep(100 * Integer.parseInt(minutes.substring(0, 1)));

                            line = br.readLine();
                        }

                    } finally {
                        br.close();
                    }

                    os.flush();
                    os.close();
                    System.out.println("Done.");

                } finally {
                    if (sock != null) {
                        sock.close();
                    }
                }
            }
        } finally {
            if (servsock != null) {
                servsock.close();
            }
        }
    }

}
