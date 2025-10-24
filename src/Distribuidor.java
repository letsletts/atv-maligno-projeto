import java.io.*;
import java.net.*;
import java.util.*; // Importado para Scanner e Arrays
import java.util.concurrent.*;

public class Distribuidor {
    // Hard-coded: IPs (ou host:porta) dos receptores. Para teste local, use "localhost:12345", "localhost:12346", ...
    // Ao rodar em máquinas diferentes, substitua pelos IPs reais (ex: "192.168.0.10:12345").
    private static final String[] RECEPTORS = {
        "localhost:12345",
        "localhost:12346",
        "localhost:12347"
    };

    private static final int BLOCKS_PER_SERVER = 8; // quantos blocos criamos por servidor (ajustável)
    private static final int DEFAULT_VECTOR_SIZE = 5_000_000; // exemplo de vetor grande (ajustável)

    public static void main(String[] args) throws Exception {
        
        // ADICIONADO: try-with-resources para o Scanner fechar automaticamente
        try (Scanner scanner = new Scanner(System.in)) {

            int vectorSize = DEFAULT_VECTOR_SIZE;
            if (args.length >= 1) {
                try { vectorSize = Integer.parseInt(args[0]); } catch (NumberFormatException ignored) {}
            }

            System.out.printf("[D] Iniciando Distribuidor. Vetor de tamanho %,d%n", vectorSize);
            Random rnd = new Random();

            // 1) gera o vetor
            byte[] grandeVetor = new byte[vectorSize];
            for (int i = 0; i < vectorSize; i++) {
                // valores entre -100 e 100
                grandeVetor[i] = (byte)(rnd.nextInt(201) - 100);
            }

            // --- FUNCIONALIDADE ADICIONADA (Requisito 6) ---
            System.out.print("[D] Deseja imprimir o vetor gerado? (S/N): ");
            String respostaPrint = scanner.nextLine();
            if (respostaPrint.trim().equalsIgnoreCase("S")) {
                // Cuidado: vetores muito grandes podem travar o terminal
                if (vectorSize > 1000) {
                     System.out.println("[D] Imprimindo os primeiros 1000 elementos: ");
                     System.out.println(Arrays.toString(Arrays.copyOf(grandeVetor, 1000)) + "...");
                } else {
                     System.out.println("[D] Vetor: " + Arrays.toString(grandeVetor));
                }
            }
            // --- FIM DA FUNCIONALIDADE ---


            // 2) escolhe aleatoriamente uma posição e o valor procurado
            int pos = rnd.nextInt(vectorSize);
            byte procurado = grandeVetor[pos];
            System.out.printf("[D] Número escolhido aleatoriamente (posição %d) = %d%n", pos, procurado);

            // --- FUNCIONALIDADE ADICIONADA (Requisito 6) ---
            System.out.print("[D] Deseja contar um número inexistente (111)? (S/N): ");
            String respostaContar = scanner.nextLine();
            if (respostaContar.trim().equalsIgnoreCase("S")) {
                procurado = (byte) 111;
                System.out.printf("[D] Número a ser procurado foi alterado para %d (inexistente).%n", procurado);
            }
            // --- FIM DA FUNCIONALIDADE ---


            // 3) dividimos o vetor em blocos
            int totalServers = RECEPTORS.length;
            int totalBlocks = totalServers * BLOCKS_PER_SERVER;
            int blockSize = Math.max(1, (vectorSize + totalBlocks - 1) / totalBlocks);

            System.out.printf("[D] Dividindo em %d blocos de ~%d elementos (blockSize=%d)%n",
                    totalBlocks, blockSize, blockSize);

            List<BlockingQueue<Pedido>> queues = new ArrayList<>();
            for (int i = 0; i < totalServers; i++) queues.add(new LinkedBlockingQueue<>());

            int serverIndex = 0;
            for (int i = 0; i < vectorSize; i += blockSize) {
                int end = Math.min(vectorSize, i + blockSize);
                int len = end - i;
                byte[] sub = new byte[len];
                System.arraycopy(grandeVetor, i, sub, 0, len);
                Pedido p = new Pedido(sub, procurado);
                queues.get(serverIndex).add(p);
                serverIndex = (serverIndex + 1) % totalServers;
            }

            // 4) cria uma thread por servidor
            ExecutorService executors = Executors.newFixedThreadPool(totalServers);
            List<Future<Integer>> futures = new ArrayList<>();

            long t0Total = System.nanoTime();

            for (int i = 0; i < totalServers; i++) {
                final int idx = i;
                Callable<Integer> task = () -> {
                    String[] parts = RECEPTORS[idx].split(":");
                    String host = parts[0];
                    int port = (parts.length > 1) ? Integer.parseInt(parts[1]) : 12345;
                    int localCountSum = 0;

                    try (Socket socket = new Socket(host, port);
                         ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                         ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                        System.out.printf("[D] Conectado a %s:%d (server %d)%n", host, port, idx);

                        BlockingQueue<Pedido> q = queues.get(idx);
                        Pedido p;
                        while ((p = q.poll(500, TimeUnit.MILLISECONDS)) != null) {
                            out.writeObject(p);
                            out.flush();
                            out.reset();

                            Object o = in.readObject();
                            if (o instanceof Resposta r) {
                                System.out.printf("[D] Resposta recebida do %s:%d -> %d%n", host, port, r.getContagem());
                                localCountSum += r.getContagem();
                            } else {
                                System.out.printf("[D] Objeto inesperado na resposta de %s:%d: %s%n",
                                        host, port, o.getClass().getName());
                            }
                        }
                        
                        ComunicadoEncerramento fim = new ComunicadoEncerramento();
                        out.writeObject(fim);
                        out.flush();
                        out.reset();
                        System.out.printf("[D] Enviado ComunicadoEncerramento a %s:%d%n", host, port);

                    } catch (IOException | ClassNotFoundException | NumberFormatException ex) {
                        System.err.printf("[D] Erro ao comunicar com %s: %s%n", RECEPTORS[idx], ex.getMessage());
                    }
                    return localCountSum;
                };

                futures.add(executors.submit(task));
            }

            // aguarda e soma resultados
            int finalCount = 0;
            for (Future<Integer> f : futures) {
                try {
                    finalCount += f.get();
                } catch (InterruptedException | ExecutionException e) {
                    System.err.println("[D] Erro obtendo futuro: " + e.getMessage());
                }
            }

            long t1Total = System.nanoTime();
            double elapsedMs = (t1Total - t0Total) / 1_000_000.0;

            System.out.printf("[D] Resultado final: número %d ocorreu %d vezes (tempo total %.3f ms)%n",
                    procurado, finalCount, elapsedMs);

            executors.shutdownNow();
            
        } // Fim do try-with-resources do Scanner
    }
}