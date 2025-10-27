public class MaiorVetorAproximado {

    /**
     * Tenta alocar vetores de byte crescentes até estourar a memória,
     * para descobrir o limite prático da máquina.
     * Usa a lógica de incremento exata do arquivo original.
     * @return O tamanho (em elementos) do maior vetor de bytes que pôde ser alocado.
     */
    public static int estimar() {
        System.out.println("[Estimador] Estimando o maior tamanho possível de vetor em Java...");
        
        int tamanho = 1_000_000; // começa com 1 milhão
        int ultimoBemSucedido = 0; 
        
        while (true) { 
            try { 
                byte[] vetor = new byte[tamanho];
                ultimoBemSucedido = tamanho;
                vetor = null; // libera
                System.gc();
                
                // aumenta o tamanho em 50% para a próxima tentativa 
                if (tamanho > Integer.MAX_VALUE / 3 * 2) break;
                
                tamanho /= 2;
                tamanho *= 3;
                
                System.out.printf("[Estimador] ...Alocado com sucesso: %,d elementos%n", ultimoBemSucedido);
            } catch (OutOfMemoryError e) {
                System.out.printf("[Estimador] ...Falhou em alocar %,d elementos%n", tamanho);
                break; // Encontrou o limite
            } 
        } 
        return ultimoBemSucedido;
    }

    /**
     * O método main agora serve apenas para testar a função de estimativa
     * de forma independente.
     */
    public static void main(String[] args) {
        long inicio = System.currentTimeMillis();
        
        int maiorTamanho = estimar(); // Chama a lógica refatorada
            
        long fim = System.currentTimeMillis();
        
        // --- ERRO CORRIGIDO AQUI ---
        // Removi os que causei o erro de compilação
        
        System.out.println("\n[Estimador] Maior vetor que coube (aproximadamente): "+ 
                        String.format("%,d", maiorTamanho)); 
        System.out.printf("[Estimador] Memória estimada: %.2f MB%n",
                        maiorTamanho * 1.0 / (1024 * 1024)); 
        System.out.printf("[Estimador] Tempo total: %.2f segundos%n", (fim - inicio) / 1000.0);
    } 
}