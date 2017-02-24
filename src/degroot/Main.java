package degroot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main class for the web request aggregation program. User input is handled and sanitized here.	
 */
public class Main {
	
	private static final String THREADS_ARG = "-t";

	private static Integer numThreads = 3;

	public static void main(String[] args) {
		if (args.length == 0 || args.length > 3) {
			usage();
		}
		
		Set<Path> paths = new HashSet<>();
		Path dataFile = null;

		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals(THREADS_ARG)) {
				try {
					numThreads = Integer.valueOf(args[++i]);
				} catch (Exception e) {
					usage();
				}
			} else {
				Path path = Paths.get(arg);
				
				if (!Files.isReadable(path)) {
					System.err.println("The file at " + path.toString() + " is nonexistent or not readable");
					System.exit(1);
				}
				
				if (Files.isDirectory(path)) {
					try {
						//Try to add all the .tsv files
						Files.list(path).forEach(p -> {
							if (p.toString().endsWith(Aggregator.DATA_SUFFIX)) {
								paths.add(p);
							} 
						});
						
						dataFile = path.resolve(Aggregator.DATA_FILE);
					} catch (IOException e) {
						System.err.println("Error reading the directory at " + path.toString());
						System.exit(1);
					}
				} else {
					System.err.println("Data path argument must be a directory");
					System.exit(1);
				}
			}
		}
		
		Aggregator aggregator = new Aggregator(paths, numThreads, dataFile);
		
		final AtomicBoolean completed = new AtomicBoolean(false);
		Thread hook = new Thread(() -> {
			if (completed.get()) {
				return;
			}
			int terms = 1;
			aggregator.terminate();
			
			while (!completed.get()) {
				try {
					Thread.sleep(10);
				} catch (Exception e) {
					//They're serious about terminating
					if (terms < 2) {
						System.err.println("Almost done...");
						terms++;
					} else {
						//Fine
						System.exit(15);
					}
				}
			}
		});
		
		Runtime.getRuntime().addShutdownHook(hook);
		
		try {
			Map<String, DomainStat> result = aggregator.getStats();
			if (!aggregator.isTerminated()) {
				formatResults(result);
			}
		} catch (IOException e) {
			System.err.println(String.format("Error accessing file: %s", e.getMessage()));
			e.printStackTrace(System.err);
		} finally {
			completed.set(true);
		}
		
		System.exit(0);
	}

	private static void formatResults(Map<String, DomainStat> stats) {
		System.out.println("Domain\tTotal Requests\tAverage Requests/hr\tMax Requests/hr");
		stats.forEach((domain, stat) -> {
			System.out.println(String.format("%s\t%d\t%f\t%d", domain, stat.totalRequests, 
					stat.averageRequestsPerHour, stat.maxRequestsPerHour));
		});
	}

	private static void usage() {
		System.out.println("Usage: java -jar degroot.jar [-t $numThreads] pathToDataDir");
		System.exit(1);
	}
}