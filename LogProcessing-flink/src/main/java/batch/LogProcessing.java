package batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

//Autor: Germano Bertoncello     e-mail: germano.bertoncello@gmail.com

/* LogProcessing é aplicação que analisa um log (convertido para CSV) de um servidor HTTP e retorna o número de acessos dos host ao servidor.*/

public final class LogProcessing {

    public static void main(final String[] args) throws Exception {


        final String filename;
        try {
            // acessa os argumentos da linha de comando
            final ParameterTool params = ParameterTool.fromArgs(args);
            if (!params.has("filename")) {
                filename = "/home/hadoop/log.csv";// caminho do arquivo log
                System.err.println("No filename specified. Please run 'LogProcessing " +
                        "--filename <filename>, where filename is the name of the dataset in CSV format");
            } else {
                filename = params.get("filename");
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'CrimeDistrict " +
                    "--filename <filename>, where filename is the name of the dataset in CSV format");
            return;
        }

        // A ExecutionEnvironment é o contexto no qual o programa é executado.
        // Usando local environment irá executar na na JVM atual,
        // enquanto remote environment irá executar em um cluster remoto.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // cria um dataset baseado na primeira coluna do arquivo csv de entrada
        DataSet<Tuple1<String>> rawdata = env.readCsvFile(filename)
                .includeFields("1")
		.ignoreInvalidLines()
                .types(String.class);



        // aplica um map-reduce para contar o número de acessos de um Host/IP ao servidor HTTP
        DataSet<Tuple2<String, Integer>> result = rawdata
                .flatMap(new Counter()) //mapeia para uma tupla (host, 1(int))
                .groupBy(0) // agrupa por host
                .sum(1); // acumula acessos

        result.print();
    }


/*---------------------------------------------------------------------------------------------------------------*/

    /**
     * Classe para traduzir as linhas da entrada em tuplas (host, 1(int))
     */
    private final static class Counter
            implements FlatMapFunction<Tuple1<String>, Tuple2<String, Integer>> {

        public void flatMap(final Tuple1<String> value, final Collector<Tuple2<String, Integer>> out) {
            out.collect(new Tuple2<String, Integer>(value.f0, 1));
        }
    }


}
