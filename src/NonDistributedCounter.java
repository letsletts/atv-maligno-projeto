public class NonDistributedCounter {
    public static void main(String[] args) {
        int size = 5_000_000;
        if (args.length >= 1) {
            try { size = Integer.parseInt(args[0]); } catch (NumberFormatException ignored) {}
        }
        java.util.Random rnd = new java.util.Random();
        
        // Corrigido para usar byte[] [cite: 3, 27]
        byte[] vetor = new byte[size];
        for (int i = 0; i < size; i++) {
            // Corrigido para castar para byte 
            vetor[i] = (byte)(rnd.nextInt(201) - 100);
        }

        int pos = rnd.nextInt(size);
        // Corrigido para usar byte 
        byte procurado = vetor[pos];
        System.out.printf("[SEQ] Número escolhido (posição %d) = %d%n", pos, procurado);

        long t0 = System.nanoTime();
        int c = 0;
        // Corrigido para iterar sobre byte
        for (byte v : vetor) if (v == procurado) c++;
        long t1 = System.nanoTime();
        System.out.printf("[SEQ] Contagem = %d, tempo (ms) = %.3f%n", c, (t1 - t0) / 1_000_000.0);
    }
}