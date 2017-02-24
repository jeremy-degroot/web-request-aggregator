package degroot;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Aggregates the web request stats over a number of paths
 */
public class Aggregator {
	public static final String DATA_SUFFIX = ".tsv";
	public static final Path DATA_FILE = Paths.get("partial.obj");


	private static final String COMPLETED_SUFFIX = ".bak";
	private static final int WAIT_MILLIS = 100;
	
	private static final int TIME = 0;
	private static final int NAME = 1;

	private Integer threads;
	private boolean isTerminated = false;
	private Set<Path> paths;
	private Path writeDataPath;
	
	private final ConcurrentHashMap<DomainHour, LongAdder> domainHourRequests;
	
	public Aggregator(Set<Path> paths, Integer threads, Path partialDataPath) {
		this.threads = threads;
		this.paths = paths;
		domainHourRequests = initializeDomainHour(partialDataPath);
		this.writeDataPath = partialDataPath;
	}
	
	public void terminate() {		
		System.err.println("Completing current file and terminating");
		isTerminated = true;
	}
	
	@SuppressWarnings("unchecked")
	private ConcurrentHashMap<DomainHour, LongAdder> initializeDomainHour(Path dataFilePath){		
		if (!Files.exists(dataFilePath)) {
			return new ConcurrentHashMap<>();
		}
		
		try {
			InputStream in = Files.newInputStream(dataFilePath);
			ConcurrentHashMap<DomainHour, LongAdder> domainHourRequestsTmp = 
					(ConcurrentHashMap<DomainHour, LongAdder>) new ObjectInputStream(in).readObject();
			return domainHourRequestsTmp;
		} catch (Exception e) {
			System.err.println("Error reading data file, proceeding without saved data");
			return new ConcurrentHashMap<>();
		}
	}
	
	public Map<String, DomainStat> getStats() throws IOException {
		Map<String, DomainStat> results = new HashMap<>();
		Iterator<Path> iter = paths.iterator();
		while (iter.hasNext()) {
			Path p = iter.next();
			if (!p.toString().endsWith(DATA_SUFFIX)) {
				System.out.println("Skipping " + p);
				continue;
			}
			
			long timeout = 0;
			Stream<String> lines = Files.newBufferedReader(p).lines();
			Iterator<String> lineIterator = lines.iterator();
			ExecutorService executor = Executors.newFixedThreadPool(threads);
			while (lineIterator.hasNext()) {
				String line = lineIterator.next();
				executor.submit(() -> {
					String[] fields = line.split("\\s+");
					String domain = fields[NAME];
					Double time = Double.valueOf(fields[TIME]);
					Instant instant = Instant.ofEpochMilli((long) (time * 1000));
					Instant hour = instant.truncatedTo(ChronoUnit.HOURS);
					DomainHour domainHour = new DomainHour(domain, hour);
					domainHourRequests.computeIfAbsent(domainHour, k -> new LongAdder()).increment();
					try {
						Thread.sleep(WAIT_MILLIS);
					} catch (InterruptedException e) {
						//Disregard, if it was a SIGTERM it'll be handled by the main thread
					}
				});
				
				//Estimate wait time for this whole thing to finish, with a little fudge
				timeout += WAIT_MILLIS + 2*threads;
			}
			
			executor.shutdown();
			
			//Synchronous wait to support doing one file at a time
			try {
				if (!executor.awaitTermination(timeout / threads, TimeUnit.MILLISECONDS)) {
					System.err.println("Current file " + p + " did not process in the alotted time");
				}
			} catch (InterruptedException e) {
				//Disregard, if it was a SIGTERM it'll be handled by the main thread
			}
			
			//Rename so we can easily tell which ones were processed and re-run the same command
			Files.move(p, Paths.get(p.toString() + COMPLETED_SUFFIX));
			
			if (isTerminated) {				
				//I'd prefer to use JSON over serialization here but these structures without Gson is a pain
				OutputStream out = Files.newOutputStream(this.writeDataPath);
				ObjectOutputStream objOut = new ObjectOutputStream(out);
				objOut.writeObject(domainHourRequests);
				objOut.close();
				
				return null;
			}
		}
		
		for (Entry<DomainHour, LongAdder> keyValue : domainHourRequests.entrySet()) {
			String domain = keyValue.getKey().domain;
			LongAdder hourStats = keyValue.getValue();

			if (!results.containsKey(domain)) {
				DomainStat stats = new DomainStat(hourStats.intValue());
				results.put(domain, stats);
			} else {
				results.get(domain).update(hourStats.intValue());
			}
		}

		return results;
	}

	public boolean isTerminated() {
		return isTerminated;
	}
}