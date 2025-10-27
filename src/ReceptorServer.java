import java.io.*;
import java.net.*;
import java.util.List; // Importar List
import java.util.concurrent.*;

public class ReceptorServer {
    private static final int PORT = 12345; // porta fixa sugerida
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

        ConnectionHandler(Socket socket) {
            this.socket = socket;
            int procs = Runtime.getRuntime().availableProcessors();
            workerPool = Executors.newFixedThreadPool(Math.max(1, procs));
            System.out.printf("[R] Handler criado (pool=%d threads)%n", procs);
        }

        @Override
        public void run() {
            try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                Object obj;
                while ((obj = in.readObject()) != null) {
                    
                    // Lógica original para Pedido individual (mantida por segurança)
                    if (obj instanceof Pedido p) {
                        System.out.printf("[R] Pedido (individual) recebido (%d elementos) procurado=%d%n",
                                p.getNumeros().length, p.getProcurado());

                        long t0 = System.nanoTime();
                        int result = contarEmParalelo(p.getNumeros(), p.getProcurado());
                        long t1 = System.nanoTime();

                        System.out.printf("[R] Contagem local (individual) completa: %d (tempo %.3f ms)%n",
                                result, (t1 - t0) / 1_000_000.0);

                        Resposta resp = new Resposta(result);
                        out.writeObject(resp);
                        out.flush();
                        out.reset();
                    } 
                    
                    // --- NOVA LÓGICA DE BATCH ---
                    else if (obj instanceof List) {
                        // Faz um cast seguro para List<Pedido>
                        @SuppressWarnings("unchecked") // Ignora o aviso de cast
                        List<Pedido> batch = (List<Pedido>) obj;

                        if (batch.isEmpty()) {
                            System.out.println("[R] Recebido batch vazio.");
                            continue;
                        }

                        System.out.printf("[R] Batch de %d pedidos recebido. Processando...%n", batch.size());
                        long t0 = System.nanoTime();
                        int somaDoBatch = 0;

                        // Processa cada pedido do batch
                        for (Pedido p : batch) {
                            // A contagem de cada pedido JÁ é paralela (usa o pool)
                            somaDoBatch += contarEmParalelo(p.getNumeros(), p.getProcurado());
                        }
                        
                        long t1 = System.nanoTime();
                        System.out.printf("[R] Contagem do batch completa: %d (tempo %.3f ms)%n",
                                somaDoBatch, (t1 - t0) / 1_000_000.0);

                        // Envia UMA resposta com o total
                        Resposta resp = new Resposta(somaDoBatch);
                        out.writeObject(resp);
                        out.flush();
                        out.reset();
                    }
                    // --- FIM DA NOVA LÓGICA ---
                    
                    else if (obj instanceof ComunicadoEncerramento) {
                        System.out.println("[R] ComunicadoEncerramento recebido. Fechando conexão atual.");
                        break; // fecha handler
                    } else {
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

        private int contarEmParalelo(byte[] vetor, byte procurado) throws InterruptedException, ExecutionException {
            int n = vetor.length;
            int procs = Runtime.getRuntime().availableProcessors();
            int chunk = Math.max(1, (n + procs - 1) / procs);

            ExecutorService pool = workerPool;
            java.util.List<Future<Integer>> futures = new java.util.ArrayList<>();

            for (int i = 0; i < n; i += chunk) {
                final int start = i;
                final int end = Math.min(n, i + chunk);
                futures.add(pool.submit(() -> {
                    int c = 0;
                    for (int j = start; j < end; j++)
                        if (vetor[j] == procurado) c++;
                    return c;
                }));
            }

            int total = 0;
            for (Future<Integer> f : futures) total += f.get();
            return total;
        }
    }
}