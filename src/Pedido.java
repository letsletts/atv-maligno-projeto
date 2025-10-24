// Corrigido para usar byte[] e byte, conforme especificação 
public class Pedido extends Comunicado {
    private static final long serialVersionUID = 1L;
    private final byte[] numeros; // Alterado de int[] para byte[] 
    private final byte procurado; // Alterado de int para byte 

    public Pedido(byte[] numeros, byte procurado) { // Construtor atualizado
        this.numeros = numeros;
        this.procurado = procurado;
    }

    public byte[] getNumeros() { // Retorno atualizado
        return numeros;
    }

    public byte getProcurado() { // Retorno atualizado
        return procurado;
    }

    // conta linear (usado caso receptor queira usar sem dividir)
    public int contar() {
        int c = 0;
        for (byte v : numeros) { // Loop atualizado para byte
            if (v == procurado) c++;
        }
        return c;
    }
}