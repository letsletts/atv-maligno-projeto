import java.io.*;
import java.net.*;
// import java.util.List; // REMOVIDO: Import desnecessário
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
                    
                    if (obj instanceof Pedido p) {
                        System.out.printf("[R] Pedido recebido (%d elementos). Delegando ao pool...%n", p.getNumeros().length);
                        
                        Runnable task = () -> {
                            try {
                                int result = contarEmParalelo(p.getNumeros(), p.getProcurado());
                                Resposta resp = new Resposta(result);

                                synchronized (outputLock) {
                                    out.writeObject(resp);
                                    out.flush();
                                    out.reset();
                                }
                                System.out.printf("[R] ...Resposta para o pedido de %d elementos enviada (Resultado=%d)%n", p.getNumeros().length, result);

                            } catch (Exception e) { // Este catch trata o InterruptedException/ExecutionException
                                System.err.println("[R] Erro em uma tarefa de contagem: " + e.getMessage());
                            }
                        };
                        
                        workerPool.submit(task); 
                    
                    } 
                    
                    else if (obj instanceof ComunicadoEncerramento) {
                        System.out.println("[R] ComunicadoEncerramento recebido. Fechando conexão atual.");
                        break; 
                    } else {
                        System.out.println("[R] Objeto desconhecido recebido: " + obj.getClass().getName());
                    }
                }
            } catch (EOFException eof) {
                System.out.println("[R] Cliente fechou a conexão.");
                
            // --- CORREÇÃO AQUI ---
            // Removido 'InterruptedException' e 'ExecutionException'
            } catch (IOException | ClassNotFoundException e) { 
                System.err.println("[R] Erro no handler: " + e.getMessage());
            // --- FIM DA CORREÇÃO ---
                
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {}
                workerPool.shutdownNow();
                System.out.println("[R] Handler finalizado.");
            }
        }

        // Este método não mudou
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