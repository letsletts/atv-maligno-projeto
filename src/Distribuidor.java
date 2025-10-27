import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Distribuidor {

    // --- CLASSE INTERNA (ajudante) para guardar as "tarefas" ---
    private static class Intervalo {
        final int inicio;
        final int fim;

        Intervalo(int inicio, int fim) {
            this.inicio = inicio;
            this.fim = fim;
        }
    }
    
    // --- CLASSE INTERNA DA THREAD (para cumprir o requisito Thread.join()) ---
    static class ContadorThread extends Thread {
        private final String host;
        private final int port;
        private final BlockingQueue<Intervalo> queue;
        private final byte[] grandeVetor;
        private final byte procurado;
        
        private int contagemParcial = 0; // Armazena o resultado

        public ContadorThread(String host, int port, BlockingQueue<Intervalo> queue, byte[] grandeVetor, byte procurado) {
            this.host = host;
            this.port = port;
            this.queue = queue;
            this.grandeVetor = grandeVetor;
            this.procurado = procurado;
        }

        // --- MÉTODO RUN ATUALIZADO (LÓGICA ASSÍNCRONA) ---
        @Override
        public void run() {
            int localCountSum = 0;
            long threadId = Thread.currentThread().threadId();
            int pedidosEnviados = 0; // Contador para saber quantas respostas esperar

            try (Socket socket = new Socket(host, port);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                System.out.printf("[D] Conectado a %s:%d (Thread %d)%n", host, port, threadId);

                Intervalo inter;
                
                // --- 1. LOOP DE ENVIO (NÃO-BLOQUEANTE) ---
                // Envia todos os pedidos da fila o mais rápido possível
                while ((inter = queue.poll(500, TimeUnit.MILLISECONDS)) != null) {
                    
                    int len = inter.fim - inter.inicio;
                    byte[] sub = new byte[len];
                    System.arraycopy(grandeVetor, inter.inicio, sub, 0, len);
                    Pedido p = new Pedido(sub, procurado);
                    
                    out.writeObject(p);
                    out.flush();
                    out.reset(); // Importante para o ObjectOutputStream
                    
                    pedidosEnviados++; // Conta quantos foram enviados
                }
                System.out.printf("[D] (Thread %d) Enviou %d pedidos. Aguardando respostas...%n", threadId, pedidosEnviados);

                // --- 2. LOOP DE RECEBIMENTO (BLOQUEANTE) ---
                // Agora, espera (bloqueia) por EXATAMENTE o número de respostas enviadas
                for (int i = 0; i < pedidosEnviados; i++) {
                    Object o = in.readObject(); // Bloqueia e espera a próxima resposta
                    
                    if (o instanceof Resposta r) {
                        localCountSum += r.getContagem();
                    } else {
                        System.out.printf("[D] Objeto inesperado na resposta de %s:%d: %s%n",
                                host, port, o.getClass().getName());
                    }
                }
                System.out.printf("[D] (Thread %d) Recebeu todas as %d respostas.%n", threadId, pedidosEnviados);

                // --- 3. FIM ---
                // Envia o comunicado de encerramento
                ComunicadoEncerramento fim = new ComunicadoEncerramento();
                out.writeObject(fim);
                out.flush();
                out.reset();
                System.out.printf("[D] Enviado ComunicadoEncerramento a %s:%d (Thread %d)%n", 
                                  host, port, threadId);

            } catch (IOException | ClassNotFoundException | NumberFormatException | InterruptedException ex) {
                System.err.printf("[D] Erro ao comunicar com %s:%d (Thread %d): %s%n", 
                                  host, port, threadId, ex.getMessage());
            }
            
            this.contagemParcial = localCountSum;
        }
        // --- FIM DA ATUALIZAÇÃO DO RUN ---
        
        // Getter para a main thread coletar o resultado após o join
        public int getContagemParcial() {
            return contagemParcial;
        }
    }
    // --- FIM DA CLASSE INTERNA DA THREAD ---


    // IPs dos receptores. Mude para os IPs reais no teste distribuído.
    private static final String[] RECEPTORS = {
        "localhost:12345",
        "localhost:12346",
        "localhost:12347"
    };

    private static final int BLOCKS_PER_SERVER = 8; 

    public static void main(String[] args) throws Exception {
        
        try (Scanner scanner = new Scanner(System.in)) {

            int tamanhoMaximoEstimado = MaiorVetorAproximado.estimar(); 
            
            int tamanhoMaximo = (int)(tamanhoMaximoEstimado * 0.95); 
            
            System.out.printf("[D] Limite de vetor estimado: %,d elementos (%.2f MB)%n", 
                                tamanhoMaximoEstimado, tamanhoMaximoEstimado / (1024.0 * 1024.0));
            System.out.printf("[D] Limite de segurança (95%%) a ser usado: %,d elementos (%.2f MB)%n%n", 
                                tamanhoMaximo, tamanhoMaximo / (1024.0 * 1024.0));


            while (true) {
                
                int vectorSize = tamanhoMaximo; 
                System.out.printf("[D] Deseja definir um tamanho para o vetor? (Padrão/Máx: %,d) (S/N): ", tamanhoMaximo);
                String respostaTamanho = scanner.nextLine();
                if (respostaTamanho.trim().equalsIgnoreCase("S")) {
                    System.out.printf("[D] Digite o tamanho desejado (limite: %,d): ", tamanhoMaximo);
                    try {
                        int inputSize = Integer.parseInt(scanner.nextLine());
                        if (inputSize > 0 && inputSize <= tamanhoMaximo) {
                            vectorSize = inputSize;
                        } else {
                            System.out.printf("[D] Tamanho inválido ou maior que o limite. Usando o máximo (%,d).%n", tamanhoMaximo);
                            vectorSize = tamanhoMaximo;
                        }
                    } catch (NumberFormatException e) {
                        System.out.printf("[D] Entrada inválida. Usando o máximo (%,d).%n", tamanhoMaximo);
                        vectorSize = tamanhoMaximo;
                    }
                }
                
                if (args.length >= 1) { 
                    try { 
                        int argSize = Integer.parseInt(args[0]);
                        if (argSize > 0 && argSize <= tamanhoMaximo) {
                            vectorSize = argSize;
                        }
                    } catch (NumberFormatException ignored) {}
                    args = new String[0]; 
                }

                System.out.printf("[D] Iniciando. Vetor de tamanho %,d%n", vectorSize);
                Random rnd = new Random();

                byte[] grandeVetor = new byte[vectorSize]; 
                for (int i = 0; i < vectorSize; i++) {
                    grandeVetor[i] = (byte)(rnd.nextInt(201) - 100); 
                }

                System.out.print("[D] Deseja imprimir o vetor gerado? (S/N): ");
                String respostaPrint = scanner.nextLine();
                if (respostaPrint.trim().equalsIgnoreCase("S")) {
                    if (vectorSize > 1000) {
                        System.out.println("[D] Imprimindo os primeiros 1000 elementos: ");
                        System.out.println(Arrays.toString(Arrays.copyOf(grandeVetor, 1000)) + "...");
                    } else {
                        System.out.println("[D] Vetor: " + Arrays.toString(grandeVetor));
                    }
                }

                int pos = rnd.nextInt(vectorSize);
                byte procuradoAleatorio = grandeVetor[pos];
                
                System.out.printf("\n[D] O número escolhido aleatoriamente é: %d (da posição %d)%n", 
                                    procuradoAleatorio, pos);
                System.out.println("[D] Pressione ENTER para iniciar a contagem...");
                scanner.nextLine();

                System.out.println("[D] Iniciando contagem...");
                
                executarContagemDistribuida(grandeVetor, procuradoAleatorio, RECEPTORS, BLOCKS_PER_SERVER);
                
                System.out.println(String.format("%n" + "-".repeat(30) + "%n")); 

                System.out.print("[D] Deseja realizar uma segunda contagem para um número inexistente (111)? (S/N): ");
                String respostaContar = scanner.nextLine();
                if (respostaContar.trim().equalsIgnoreCase("S")) {
                    byte procuradoInexistente = (byte) 111;
                    System.out.printf("[D] Iniciando contagem para o número inexistente: %d%n", procuradoInexistente);
                    
                    executarContagemDistribuida(grandeVetor, procuradoInexistente, RECEPTORS, BLOCKS_PER_SERVER);
                }

                System.out.println(String.format("%n" + "=".repeat(40) + "%n"));
                System.out.print("[D] Deseja realizar uma NOVA CONTAGEM com um NOVO VETOR? (S/N): ");
                String respostaNovaRodada = scanner.nextLine();
                if (!respostaNovaRodada.trim().equalsIgnoreCase("S")) {
                    System.out.println("[D] Encerrando o Distribuidor.");
                    break; 
                }
                System.out.println("[D] Reiniciando... Gerando novo vetor.\n");

            } // Fim do while(true)
            
        } // Fim do try-with-resources (Scanner)
    }

    /**
     * Método refatorado para executar uma rodada de contagem distribuída.
     * --- NENHUMA MUDANÇA NESTE MÉTODO ---
     */
    private static void executarContagemDistribuida(byte[] grandeVetor, byte procurado, String[] receptors, int blocksPerServer) {
        int vectorSize = grandeVetor.length;
        int totalServers = receptors.length;
        
        int totalBlocks = totalServers * blocksPerServer; 
        int blockSize = Math.max(1, (vectorSize + totalBlocks - 1) / totalBlocks); 

        System.out.printf("[D] Dividindo em %d blocos de ~%d elementos (blockSize=%d)%n",
                totalBlocks, blockSize, blockSize);

        List<BlockingQueue<Intervalo>> queues = new ArrayList<>();
        for (int i = 0; i < totalServers; i++) queues.add(new LinkedBlockingQueue<>());

        int serverIndex = 0;
        for (int i = 0; i < vectorSize; i += blockSize) {
            int end = Math.min(vectorSize, i + blockSize);
            Intervalo inter = new Intervalo(i, end); 
            queues.get(serverIndex).add(inter); 
            serverIndex = (serverIndex + 1) % totalServers;
        }

        List<ContadorThread> threads = new ArrayList<>();
        
        long t0Total = System.nanoTime();

        // 1. Loop para CRIAR e INICIAR as threads
        for (int i = 0; i < totalServers; i++) {
            String[] parts = receptors[i].split(":");
            String host = parts[0];
            int port = (parts.length > 1) ? Integer.parseInt(parts[1]) : 12345;
            
            final BlockingQueue<Intervalo> q = queues.get(i);
            
            // Cria a thread personalizada
            ContadorThread t = new ContadorThread(host, port, q, grandeVetor, procurado);
            threads.add(t);
            
            // Inicia a thread
            t.start();
        }

        // 2. Loop para SINCRONIZAR (Thread.join()) e COLETAR resultados
        int finalCount = 0;
        for (ContadorThread t : threads) {
            try {
                // --- REQUISITO CUMPRIDO: Usando Thread.join() ---
                t.join();
                
                // Coleta o resultado após a thread terminar
                finalCount += t.getContagemParcial();
                 
            } catch (InterruptedException e) {
                System.err.println("[D] Thread principal interrompida enquanto esperava: " + e.getMessage());
            }
        }

        long t1Total = System.nanoTime();
        double elapsedMs = (t1Total - t0Total) / 1_000_000.0;

        System.out.printf("[D] Resultado final: número %d ocorreu %d vezes (tempo total %.3f ms)%n",
                procurado, finalCount, elapsedMs);
    }
}