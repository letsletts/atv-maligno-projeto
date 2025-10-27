import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Distribuidor {

    // --- CLASSE INTERNA (ajudante) para guardar as "tarefas" ---
    // É minúscula, não consome quase nada de memória
    private static class Intervalo {
        final int inicio;
        final int fim;

        Intervalo(int inicio, int fim) {
            this.inicio = inicio;
            this.fim = fim;
        }
    }
    // --- FIM DA CLASSE INTERNA ---


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
            
            // --- VOLTAMOS PARA 95% ---
            // Agora é seguro, pois não vamos duplicar a memória
            int tamanhoMaximo = (int)(tamanhoMaximoEstimado * 0.95); 
            // --- FIM DA CORREÇÃO ---
            
            System.out.printf("[D] Limite de vetor estimado: %,d elementos (%.2f MB)%n", 
                                tamanhoMaximoEstimado, tamanhoMaximoEstimado / (1024.0 * 1024.0));
            System.out.printf("[D] Limite de segurança (95%%) a ser usado: %,d elementos (%.2f MB)%n%n", 
                                tamanhoMaximo, tamanhoMaximo / (1024.0 * 1024.0));


            mainLoop: // Rótulo para o loop principal
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

                // Esta alocação (linha 95) agora deve ser segura
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
                
                // --- MUDANÇA: Passamos o grandeVetor para o método de execução ---
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
                    break mainLoop; 
                }
                System.out.println("[D] Reiniciando... Gerando novo vetor.\n");

            } // Fim do mainLoop (while(true))
            
        } // Fim do try-with-resources (Scanner)
    }

    /**
     * Método refatorado para executar uma rodada de contagem distribuída.
     * --- AGORA RECEBE O GRANDE VETOR ---
     */
    private static void executarContagemDistribuida(byte[] grandeVetor, byte procurado, String[] receptors, int blocksPerServer) {
        int vectorSize = grandeVetor.length;
        int totalServers = receptors.length;
        
        int totalBlocks = totalServers * blocksPerServer; 
        int blockSize = Math.max(1, (vectorSize + totalBlocks - 1) / totalBlocks); 

        System.out.printf("[D] Dividindo em %d blocos de ~%d elementos (blockSize=%d)%n",
                totalBlocks, blockSize, blockSize);

        // --- MUDANÇA: A Fila agora é de "Intervalo", não de "Pedido" ---
        List<BlockingQueue<Intervalo>> queues = new ArrayList<>();
        for (int i = 0; i < totalServers; i++) queues.add(new LinkedBlockingQueue<>());

        int serverIndex = 0;
        // --- MUDANÇA: Não criamos cópias aqui, apenas "Intervalos" ---
        for (int i = 0; i < vectorSize; i += blockSize) {
            int end = Math.min(vectorSize, i + blockSize);
            Intervalo inter = new Intervalo(i, end); // Objeto minúsculo
            queues.get(serverIndex).add(inter); // Adiciona o objeto minúsculo
            serverIndex = (serverIndex + 1) % totalServers;
        }
        // --- NENHUM OutOfMemoryError AQUI ---

        ExecutorService executors = Executors.newFixedThreadPool(totalServers);
        List<Future<Integer>> futures = new ArrayList<>();

        long t0Total = System.nanoTime();

        for (int i = 0; i < totalServers; i++) {
            final int idx = i;
            // --- MUDANÇA: Passamos a fila de Intervalo para a thread ---
            final BlockingQueue<Intervalo> q = queues.get(idx);
            
            Callable<Integer> task = () -> {
                String[] parts = receptors[idx].split(":");
                String host = parts[0];
                int port = (parts.length > 1) ? Integer.parseInt(parts[1]) : 12345;
                int localCountSum = 0;

                try (Socket socket = new Socket(host, port);
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                    System.out.printf("[D] Conectado a %s:%d (server %d)%n", host, port, idx);

                    // --- MUDANÇA: A thread agora consome "Intervalo" ---
                    Intervalo inter;
                    while ((inter = q.poll(500, TimeUnit.MILLISECONDS)) != null) {
                        
                        // --- MUDANÇA: A CÓPIA É FEITA AQUI, DENTRO DA THREAD ---
                        int len = inter.fim - inter.inicio;
                        byte[] sub = new byte[len];
                        System.arraycopy(grandeVetor, inter.inicio, sub, 0, len);
                        Pedido p = new Pedido(sub, procurado);
                        // --- FIM DA MUDANÇA ---
                        
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
                        
                        // 'sub' e 'p' são liberados da memória no final deste loop
                    }
                    
                    ComunicadoEncerramento fim = new ComunicadoEncerramento();
                    out.writeObject(fim);
                    out.flush();
                    out.reset();
                    System.out.printf("[D] Enviado ComunicadoEncerramento a %s:%d%n", host, port);

                } catch (IOException | ClassNotFoundException | NumberFormatException ex) {
                    System.err.printf("[D] Erro ao comunicar com %s: %s%n", receptors[idx], ex.getMessage());
                }
                return localCountSum;
            };

            futures.add(executors.submit(task));
        }

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
    }
}