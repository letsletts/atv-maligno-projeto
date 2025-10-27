import java.io.*;
import java.net.*;
import java.util.ArrayList; // Importar ArrayList
import java.util.List; // Importar List
import java.util.concurrent.*;

public class ReceptorServer {
    private static final int PORT = 12345;
    private static final int BACKLOG = 50;

    public static void main(String[] args) {
        int port = PORT;
        if (args.length >= 1) {
            try { port = Integer.parseInt(args[0]); } catch (Exception e) { /* usa padrão */ }
        }

        System.out.printf("[R] Iniciando Receptor em porta %d%n", port);

        try (ServerSocket serverSocket = new ServerSocket(port, BACKLOG)) {
            while (true) {
                Socket client = serverSocket.accept();
                System.out.printf("[R] Conexão aceita de %s:%d%n",
                        client.getInetAddress().getHostAddress(), client.getPort());
                new Thread(new ConnectionHandler(client)).start();
            }
        } catch (IOException e) {
            System.err.println("[R] Erro no servidor: " + e.getMessage());
        }
    }

    static class ConnectionHandler implements Runnable {
        private final Socket socket;
        private final ExecutorService workerPool;
        private final Object outputLock = new Object(); 
        private ObjectOutputStream out; 

        ConnectionHandler(Socket socket) {
            this.socket = socket;
            int procs = Runtime.getRuntime().availableProcessors();
            workerPool = Executors.newFixedThreadPool(Math.max(1, procs));
            System.out.printf("[R] Handler criado (pool=%d threads)%n", procs);
        }

        @Override
        public void run() {
            try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                this.out = new ObjectOutputStream(socket.getOutputStream());

                Object obj;
                while ((obj = in.readObject()) != null) {
                    
                    // --- MUDANÇA CRUCIAL: Tratar a LISTA primeiro ---
                    if (obj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Pedido> batch = (List<Pedido>) obj; // <-- Usa Pedido.java

                        if (batch.isEmpty()) {
                            System.out.println("[R] Recebido batch vazio.");
                            // Envia uma resposta de "0" para destravar o cliente
                            synchronized(outputLock) {
                                out.writeObject(new Resposta(0)); // <-- Usa Resposta.java
                                out.flush();
                                out.reset();
                            }
                            continue;
                        }

                        System.out.printf("[R] Batch de %d pedidos recebido. Processando em paralelo...%n", batch.size());
                        long t0 = System.nanoTime();

                        // --- Processar o batch em paralelo ---
                        List<Future<Integer>> batchFutures = new ArrayList<>();
                        for (Pedido p : batch) {
                            // Envia cada pedido do batch para o pool de threads
                            batchFutures.add(workerPool.submit(() -> {
                                return p.contar(); // <-- USA O MÉTODO p.contar()
                            }));
                        }

                        // Coleta os resultados do batch
                        int somaDoBatch = 0;
                        for (Future<Integer> f : batchFutures) {
                            somaDoBatch += f.get(); // Espera cada sub-contagem do batch terminar
                        }
                        
                        long t1 = System.nanoTime();
                        System.out.printf("[R] Contagem do batch completa: %d (tempo %.3f ms)%n",
                                somaDoBatch, (t1 - t0) / 1_000_000.0);

                        // Envia UMA resposta com o total
                        Resposta resp = new Resposta(somaDoBatch); // <-- Usa Resposta.java
                        synchronized (outputLock) {
                            out.writeObject(resp);
                            out.flush();
                            out.reset();
                        }
                    }
                    
                    else if (obj instanceof ComunicadoEncerramento) { // <-- Usa ComunicadoEncerramento.java
                        System.out.println("[R] ComunicadoEncerramento recebido. Fechando conexão atual.");
                        break; 
                    } else if (!(obj instanceof List)) { // Ignora se não for a Lista ou o Encerramento
                        System.out.println("[R] Objeto desconhecido recebido: " + obj.getClass().getName());
                    }
                }
            } catch (EOFException eof) {
                System.out.println("[R] Cliente fechou a conexão.");
            } catch (IOException | ClassNotFoundException | InterruptedException | ExecutionException e) { 
                System.err.println("[R] Erro no handler: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {}
                workerPool.shutdownNow();
                System.out.println("[R] Handler finalizado.");
            }
        }
        
        // O método "contarEmParalelo" foi removido daqui pois não é mais necessário.
    }
}