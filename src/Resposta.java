public class Resposta extends Comunicado {
    private static final long serialVersionUID = 1L;
    // Corrigido para usar o tipo primitivo int 
    private final int contagem; 

    public Resposta(int contagem) {
        this.contagem = contagem;
    }

    // Corrigido para retornar o tipo primitivo int [cite: 35]
    public int getContagem() {
        return contagem;
    }
}