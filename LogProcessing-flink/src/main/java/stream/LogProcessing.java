package stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

//Autor: Germano Bertoncello     e-mail: germano.bertoncello@gmail.com

// LogProcessing é uma aplicação que analisa um log recebido através de um socket e retorna os hosts que acessaram o servidor HTTP, o número de acessos e o total de bytes enviados (dentro da janela que está sendo processada).


@SuppressWarnings("serial")
public class LogProcessing {

	public static void main(String[] args) throws Exception {

		// Configura hostname e porta do socket que será utilizado para entrada dos dados
		final String hostname;
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'LogPRocessing " +
				"--hostname <hostname> --port <port>', where hostname (localhost by default) " +
				"and port is the address of the text server");
			System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
				"type the input text into the command line");
			return;
		}

		// configura o ambiente de execução
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// conecta ao socket de entrada dos dados
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");

		// processa os dados e acumula os contadores
		DataStream<DataLog> rawdata = text

				.flatMap(new FlatMapFunction<String, DataLog>() {
					@Override
					public void flatMap(String value, Collector<DataLog> out) {
					final String[] items = value.split(",");//Separa a linha de entrada e obtém o valor em bytes do objeto do reply 
					final long size = Long.parseLong(items[7]);
					out.collect(new DataLog(items[0], 1L, size));
					}
				})

				.keyBy("host")
				.timeWindow(Time.seconds(5)) //configura a janela para 5 segundos

				.reduce(new ReduceFunction<DataLog>() {
					@Override
					public DataLog reduce(DataLog a, DataLog b) {
						return new DataLog(a.host, a.count + b.count, a.sizeObject + b.sizeObject);
					}
				});

		// Imprime os resultados finais
		rawdata.print().setParallelism(1);

		env.execute("Log Processing");
	}

	// ------------------------------------------------------------------------


	public static class DataLog {

		public String host;
		public long count;
		public long sizeObject;

		public DataLog() {}

		public DataLog(String host, long count, long sizeObject) {
			this.host = host;
			this.count = count;
			this.sizeObject = sizeObject;
		}

		@Override
		public String toString() {
			return host + " : Number of requests = " + count + " : Total (bytes) = " + sizeObject;
		}
	}
}
