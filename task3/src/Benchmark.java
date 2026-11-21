import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Homework 3: throughput benchmark for the blocking_data workload (successor of Blocking / NoLiveLocks).
 *
 * <ul>
 *   <li>Workload: every transaction of the stream is a 3-point read (60 %), an insert (10 %), a full
 *       table scan (10 %) or a 3-row write (20 %), see {@link Txn#at}.</li>
 *   <li>Continuous stream: closed loop. Each of the T worker threads starts the next transaction of the
 *       stream as soon as its previous one committed, so at most T transactions are in flight.</li>
 *   <li>All transactions commit: deadlocks/serialization failures are retried with the same values;
 *       after {@link #MAX_RETRIES} aborts a write runs under an exclusive table lock (no starvation).</li>
 *   <li>Serial execution is the same code with {@code --threads 1}.</li>
 *   <li>Optimizations are switched on one by one with {@code --opt}, see {@link Opt}.</li>
 * </ul>
 *
 * Run {@code java Benchmark --help} for the flags.
 */
public class Benchmark {

	static final int NUM_TUPLE = 50_000;
	static final double MAX_VALUE = 50.0;
	static final int QUERIES_PER_TXN = 3;
	/** after this many aborts a write takes an exclusive table lock, which bounds its restarts */
	static final int MAX_RETRIES = 3;
	/** warm-up draws from a far-away part of the stream, so the measured part always starts at index 0 */
	static final long WARMUP_STREAM_OFFSET = 1_000_000_000L;

	static final String SQL_READ = "SELECT data_value FROM blocking_data WHERE data_id = ?";
	static final String SQL_INSERT = "INSERT INTO blocking_data VALUES (?, ?, ?)";
	static final String SQL_SCAN = "SELECT sum(data_value) FROM blocking_data";
	static final String SQL_WRITE = "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?";

	enum Type { READ, INSERT, SCAN, WRITE }

	static boolean useAutocommit(Set<Opt> opts, Type type) {
		return opts.contains(Opt.AUTOCOMMIT) && (type == Type.INSERT || type == Type.SCAN);
	}

	/** The optimizations that can be switched on individually (ablation). */
	enum Opt {
		/** prepare each statement once per connection instead of once per transaction */
		STMT,
		/** buffer the console output instead of flushing every printed sum */
		PRINT,
		/** single-statement INSERT/SCAN only; multi-point READ keeps its transaction boundary */
		AUTOCOMMIT,
		/** mark READ/SCAN transactions read-only through JDBC */
		READONLY,
		/** synchronous_commit = off: a commit does not wait for the WAL flush */
		ASYNC
	}

	/**
	 * One transaction of the stream. It depends only on (seed, index), so every run and every execution
	 * strategy works off exactly the same stream, and a retry re-executes the same values.
	 */
	record Txn(int id, Type type, int[] dataIds, double[] values) {
		int insertId() { return Math.addExact(NUM_TUPLE, id); }
		static Txn at(long seed, long index) {
			SplittableRandom rnd = new SplittableRandom(seed + index * 0x9E3779B97F4A7C15L);
			double p = rnd.nextDouble();
			Type type = p < 0.6 ? Type.READ : p < 0.7 ? Type.INSERT : p < 0.8 ? Type.SCAN : Type.WRITE;
			int[] dataIds = rnd.ints(0, NUM_TUPLE).distinct().limit(QUERIES_PER_TXN).toArray();
			double[] values = rnd.doubles(QUERIES_PER_TXN, 0, MAX_VALUE).toArray();
			return new Txn(Math.toIntExact(index), type, dataIds, values);
		}
	}

	record Config(int threads, int durationSec, long count, int warmupSec, long seed, Set<Opt> opts,
			String label, Path csv, Path timeline) {

		static Config parse(String[] args) {
			int threads = 8, duration = 30, warmup = 5;
			long count = 0, seed = 42;
			Set<Opt> opts = EnumSet.noneOf(Opt.class);
			String label = "";
			Path csv = Path.of("results/runs.csv"), timeline = null;
			for (int i = 0; i < args.length; i++) {
				String flag = args[i];
				if (flag.equals("--help")) {
					System.out.println(USAGE);
					System.exit(0);
				}
				if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + flag + "\n" + USAGE);
				String value = args[++i];
				switch (flag) {
					case "--threads" -> threads = Integer.parseInt(value);
					case "--duration" -> duration = Integer.parseInt(value);
					case "--count" -> count = Long.parseLong(value);
					case "--warmup" -> warmup = Integer.parseInt(value);
					case "--seed" -> seed = Long.parseLong(value);
					case "--opt" -> opts = parseOpts(value);
					case "--label" -> label = value;
					case "--csv" -> csv = Path.of(value);
					case "--timeline" -> timeline = Path.of(value);
					default -> throw new IllegalArgumentException("unknown flag " + flag + "\n" + USAGE);
				}
			}
			if (duration < 1 || warmup < 0 || count < 0 || count > Integer.MAX_VALUE - NUM_TUPLE)
				throw new IllegalArgumentException("invalid duration, warmup or count");
			if (threads < 1) throw new IllegalArgumentException("--threads must be >= 1");
			return new Config(threads, duration, count, warmup, seed, opts, label, csv, timeline);
		}

		static Set<Opt> parseOpts(String value) {
			if (value.equals("none")) return EnumSet.noneOf(Opt.class);
			if (value.equals("all")) return EnumSet.allOf(Opt.class);
			Set<Opt> opts = EnumSet.noneOf(Opt.class);
			for (String name : value.split(",")) opts.add(Opt.valueOf(name.trim().toUpperCase(Locale.ROOT)));
			return opts;
		}

		String optsString() {
			return opts.isEmpty() ? "none" : String.join("+", opts.stream().map(o -> o.name().toLowerCase(Locale.ROOT)).toList());
		}
	}

	static final String USAGE = """
			usage: Benchmark [flags]
			  --threads N     worker threads = concurrent transactions (default 8; 1 = serial execution)
			  --duration S    measure for S seconds (default 30)
			  --count N       instead: measure until the first N stream transactions committed
			  --warmup S      warm-up seconds before measuring, not counted (default 5)
			  --seed X        stream seed (default 42)
			  --opt LIST      none | all | comma list of stmt,print,autocommit,readonly,async (default none)
			  --label TEXT    free text for the CSV row
			  --csv FILE      append the result row here (default results/runs.csv)
			  --timeline FILE write one row per second (for scripts/live_plot.py)
			Sums of reads and scans go to stdout, status lines to stderr.""";

	/** Commit counters and an atomic gauge for tasks currently being processed. */
	static final class Stats {
		final LongAdder started = new LongAdder();
		final AtomicInteger active = new AtomicInteger();
		final LongAdder[] committed = { new LongAdder(), new LongAdder(), new LongAdder(), new LongAdder() };
		final LongAdder retries = new LongAdder();
		final LongAdder escalations = new LongAdder();

		Counts snapshot() {
			return new Counts(Arrays.stream(committed).mapToLong(LongAdder::sum).toArray(),
					started.sum(), retries.sum(), escalations.sum());
		}
	}

	record Counts(long[] perType, long started, long retries, long escalations) {
		long committed() { return Arrays.stream(perType).sum(); }

		Counts minus(Counts o) {
			long[] d = new long[perType.length];
			for (int i = 0; i < d.length; i++) d[i] = perType[i] - o.perType[i];
			return new Counts(d, started - o.started, retries - o.retries, escalations - o.escalations);
		}
	}

	/** Which part of the stream a phase runs: indices [next, endIndex), stopping at deadline. */
	record Phase(String name, long endIndex, long deadlineNanos) {}

	final Config cfg;
	final Stats stats = new Stats();
	final AtomicLong nextIndex = new AtomicLong();
	final PrintStream out;
	volatile Phase phase;
	volatile boolean aborted;

	Benchmark(Config cfg) {
		this.cfg = cfg;
		// System.out flushes on every println; the buffered stream only when 64 KiB are full
		this.out = cfg.opts().contains(Opt.PRINT)
				? new PrintStream(new BufferedOutputStream(new FileOutputStream(FileDescriptor.out), 1 << 16), false)
				: System.out;
	}

	public static void main(String[] args) throws Exception {
		Config cfg = Config.parse(args);
		Benchmark b = new Benchmark(cfg);
		try (Connection guard = holdSingleRunLock()) {
			b.recreateTable();
			b.run();
		}
	}

	/** A second benchmark would drop the table under the running one, so only one may run at a time. */
	static Connection holdSingleRunLock() throws SQLException {
		Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
		try (Statement st = con.createStatement(); ResultSet rs = st.executeQuery("SELECT pg_try_advisory_lock(3)")) {
			rs.next();
			if (!rs.getBoolean(1)) {
				con.close();
				throw new IllegalStateException("another benchmark is running against this database, wait until it finished");
			}
		}
		return con; // the session-level lock is released when this connection closes
	}

	void run() throws Exception {
		List<Worker> workers = new ArrayList<>();
		for (int i = 0; i < cfg.threads(); i++) workers.add(new Worker(i)); // connections are opened before any timer
		try {
			if (cfg.warmupSec() > 0) {
				nextIndex.set(WARMUP_STREAM_OFFSET);
				runPhase(workers, new Phase("warmup", Long.MAX_VALUE, deadline(cfg.warmupSec())));
				out.flush();
				for (Worker w : workers) w.resetAfterWarmup();
				recreateTable(); // same seeded data, no warm-up inserts/updates/bloat in measurement
			}
			if ("1".equals(System.getenv("BENCHMARK_ANALYZE"))) {
				try (Statement st = workers.get(0).con.createStatement()) {
					st.execute("SELECT pg_stat_statements_reset()");
				}
			}
			for (Worker w : workers) w.con.setClientInfo("ApplicationName", "adb-task-3-measure");
			nextIndex.set(0);
			Phase measure = cfg.count() > 0
					? new Phase("measure", cfg.count(), Long.MAX_VALUE)
					: new Phase("measure", Long.MAX_VALUE, deadline(cfg.durationSec()));
			Counts before = stats.snapshot();
			long start = System.nanoTime();
			runPhase(workers, measure);
			out.flush(); // the output belongs to the transactions, so flushing it is measured too
			double seconds = (System.nanoTime() - start) / 1e9;
			report(stats.snapshot().minus(before), seconds);
		} finally {
			for (Worker w : workers) w.close();
		}
	}

	static long deadline(int seconds) {
		return System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
	}

	/** Drop, create and load the table, so every run starts without old inserts and without update bloat. */
	void recreateTable() throws SQLException {
		try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			 Statement st = con.createStatement()) {
			st.execute("DROP TABLE IF EXISTS blocking_data");
			st.execute("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer)");
			con.setAutoCommit(false);
			Random rand = new Random(cfg.seed());
			try (PreparedStatement ps = con.prepareStatement(SQL_INSERT)) {
				for (int i = 0; i < NUM_TUPLE; i++) {
					ps.setInt(1, i);
					ps.setDouble(2, rand.nextDouble() * MAX_VALUE);
					ps.setInt(3, 0);
					ps.addBatch();
				}
				ps.executeBatch();
			}
			con.commit();
			con.setAutoCommit(true);
			st.execute("VACUUM ANALYZE blocking_data");
			st.execute("CHECKPOINT"); // so no run pays for the checkpoint of the previous load
		}
		System.err.printf("loaded %d tuples, threads=%d opts=%s%n", NUM_TUPLE, cfg.threads(), cfg.optsString());
	}

	void runPhase(List<Worker> workers, Phase p) throws InterruptedException {
		phase = p;
		List<Thread> threads = workers.stream().map(w -> new Thread(w, "worker-" + p.name() + "-" + w.number)).toList();
		try (ScheduledExecutorService status = Executors.newSingleThreadScheduledExecutor()) {
			StatusPrinter printer = new StatusPrinter(p.name());
			status.scheduleAtFixedRate(printer, 1, 1, TimeUnit.SECONDS);
			threads.forEach(Thread::start);
			for (Thread t : threads) t.join();
			status.shutdownNow();
		}
		for (Worker w : workers) {
			if (w.failure != null) throw new IllegalStateException("worker " + w.number + " failed", w.failure);
		}
	}

	void report(Counts c, double seconds) throws IOException {
		double tps = c.committed() / seconds;
		System.err.printf(Locale.ROOT, "[DONE] %s threads=%d opts=%s: %d commits in %.2f s = %.1f tx/s "
				+ "(read %d, insert %d, scan %d, write %d), retries %d, exclusive runs %d%n",
				cfg.label(), cfg.threads(), cfg.optsString(), c.committed(), seconds, tps,
				c.perType()[0], c.perType()[1], c.perType()[2], c.perType()[3], c.retries(), c.escalations());
		if (cfg.count() > 0 && c.committed() != cfg.count()) {
			throw new IllegalStateException("expected " + cfg.count() + " commits, got " + c.committed());
		}
		appendCsv(cfg.csv(), "time,label,threads,opts,mode,seconds,commits,tps,read,insert,scan,write,retries,escalations,version,seed,warmup",
				String.format(Locale.ROOT, "%s,%s,%d,%s,%s,%.3f,%d,%.1f,%d,%d,%d,%d,%d,%d,2,%d,%d", LocalDateTime.now().withNano(0),
						cfg.label(), cfg.threads(), cfg.optsString(), cfg.count() > 0 ? "count" : "duration", seconds,
						c.committed(), tps, c.perType()[0], c.perType()[1], c.perType()[2], c.perType()[3],
						c.retries(), c.escalations(), cfg.seed(), cfg.warmupSec()));
	}

	static synchronized void appendCsv(Path file, String header, String row) throws IOException {
		if (file.getParent() != null) Files.createDirectories(file.getParent());
		boolean fresh = !Files.exists(file) || Files.size(file) == 0;
		if (!fresh) {
			try (var reader = Files.newBufferedReader(file)) {
				if (!header.equals(reader.readLine())) throw new IOException("CSV schema mismatch: " + file);
			}
		}
		try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
			if (fresh) w.println(header);
			w.println(row);
		}
	}

	/**
	 * Prints one status line per second. The atomic in-flight gauge counts tasks through output
	 * completion and stays between zero and T, without races between separate commit counters.
	 */
	final class StatusPrinter implements Runnable {
		final String phaseName;
		Counts last = stats.snapshot();
		int second;

		StatusPrinter(String phaseName) { this.phaseName = phaseName; }

		@Override
		public void run() {
			Counts now = stats.snapshot(), d = now.minus(last);
			last = now;
			second++;
			long inFlight = stats.active.get();
			System.err.printf("[%-7s %3ds] %6d tx/s | read %5d insert %4d scan %4d write %5d | in flight %2d | retries %d exclusive %d%n",
					phaseName, second, d.committed(), d.perType()[0], d.perType()[1], d.perType()[2], d.perType()[3],
					inFlight, d.retries(), d.escalations());
			if (cfg.timeline() == null) return;
			try {
				appendCsv(cfg.timeline(), "phase,second,tps,read,insert,scan,write,in_flight,retries,escalations,threads,label,epoch_ms",
						String.format("%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d", phaseName, second, d.committed(), d.perType()[0],
								d.perType()[1], d.perType()[2], d.perType()[3], inFlight, d.retries(), d.escalations(), cfg.threads(), cfg.label(), System.currentTimeMillis()));
			} catch (IOException e) {
				System.err.println("timeline: " + e.getMessage());
			}
		}
	}

	/** One worker thread with its own connection (opened before the timer, reused for all its transactions). */
	final class Worker implements Runnable, AutoCloseable {
		final int number;
		final Connection con;
		final Map<String, PreparedStatement> preparedOnce = new HashMap<>();
		final List<PreparedStatement> perTransaction = new ArrayList<>();
		volatile Throwable failure;

		Worker(int number) throws SQLException {
			this(number, ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL));
		}

		Worker(int number, Connection connection) throws SQLException {
			this.number = number;
			this.con = connection;
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			con.setClientInfo("ApplicationName", "adb-task-3-warmup");
			if (cfg.opts().contains(Opt.ASYNC)) {
				try (Statement st = con.createStatement()) {
					st.execute("SET synchronous_commit = off");
				}
			}
		}

		@Override
		public void run() {
			try {
				long i;
				while (!aborted && System.nanoTime() < phase.deadlineNanos()
						&& (i = nextIndex.getAndIncrement()) < phase.endIndex()) {
					execute(Txn.at(cfg.seed(), i));
				}
			} catch (Throwable t) {
				failure = t;
				aborted = true; // stop the other workers, the run is invalid anyway
			}
		}

		/** Runs a transaction until it commits; the same values are re-executed on every retry. */
		void execute(Txn t) throws SQLException, InterruptedException {
			stats.active.incrementAndGet();
			try {
				executeUntilCommitted(t);
			} finally {
				stats.active.decrementAndGet();
			}
		}

		void executeUntilCommitted(Txn t) throws SQLException, InterruptedException {
			stats.started.increment();
			for (int attempt = 1; ; attempt++) {
				try {
					Double result = runOnce(t, attempt > MAX_RETRIES);
					stats.committed[t.type().ordinal()].increment();
					if (result != null) printResult(t, result);
					return;
				} catch (SQLException e) {
					if (!con.getAutoCommit()) con.rollback(); // otherwise every retry fails with "transaction is aborted"
					if (!isRetryable(e)) throw e;
					stats.retries.increment();
					backoff();
				}
			}
		}

		/** One attempt. Returns the sum to print, or null for inserts and writes. */
		Double runOnce(Txn t, boolean exclusive) throws SQLException {
			boolean autocommit = useAutocommit(cfg.opts(), t.type());
			con.setReadOnly(cfg.opts().contains(Opt.READONLY) && (t.type() == Type.READ || t.type() == Type.SCAN));
			con.setAutoCommit(autocommit);
			try {
				Double result = switch (t.type()) {
					case READ -> readTxn(t);
					case INSERT -> insertTxn(t);
					case SCAN -> scanTxn();
					case WRITE -> writeTxn(t, exclusive);
				};
				if (!autocommit) con.commit();
				return result;
			} finally {
				closePerTransactionStatements();
			}
		}

		Double readTxn(Txn t) throws SQLException {
			PreparedStatement ps = statement(SQL_READ);
			double sum = 0;
			for (int id : t.dataIds()) {
				ps.setInt(1, id);
				try (ResultSet rs = ps.executeQuery()) {
					rs.next();
					sum += rs.getDouble(1);
				}
			}
			return sum;
		}

		Double insertTxn(Txn t) throws SQLException {
			PreparedStatement ps = statement(SQL_INSERT);
			ps.setInt(1, t.insertId()); // stable across scheduling and retries; reset removes warm-up inserts
			ps.setDouble(2, t.values()[0]);
			ps.setInt(3, t.id());
			ps.executeUpdate();
			return null;
		}

		Double scanTxn() throws SQLException {
			try (ResultSet rs = statement(SQL_SCAN).executeQuery()) {
				rs.next();
				return rs.getDouble(1);
			}
		}

		Double writeTxn(Txn t, boolean exclusive) throws SQLException {
			if (exclusive) lockTableExclusive();
			PreparedStatement ps = statement(SQL_WRITE);
			// order as generated, never sorted: the sheet forbids canonical lock ordering
			for (int q = 0; q < QUERIES_PER_TXN; q++) {
				ps.setDouble(1, t.values()[q]);
				ps.setInt(2, t.id());
				ps.setInt(3, t.dataIds()[q]);
				ps.executeUpdate();
			}
			return null;
		}

		/** Escalation: blocks all other writers (readers still run), so this attempt cannot deadlock. */
		void lockTableExclusive() throws SQLException {
			try (Statement st = con.createStatement()) {
				st.execute("LOCK TABLE blocking_data IN EXCLUSIVE MODE");
			}
			stats.escalations.increment();
		}

		void printResult(Txn t, double sum) {
			out.println(t.type() + " txn " + t.id() + " sum = " + sum);
		}

		PreparedStatement statement(String sql) throws SQLException {
			if (!cfg.opts().contains(Opt.STMT)) {
				PreparedStatement ps = con.prepareStatement(sql);
				perTransaction.add(ps);
				return ps;
			}
			PreparedStatement ps = preparedOnce.get(sql);
			if (ps == null) {
				ps = con.prepareStatement(sql);
				preparedOnce.put(sql, ps);
			}
			return ps;
		}

		void closePerTransactionStatements() throws SQLException {
			for (PreparedStatement ps : perTransaction) ps.close();
			perTransaction.clear();
		}

		boolean isRetryable(SQLException e) {
			// 40P01 deadlock detected, 40001 serialization failure
			return "40P01".equals(e.getSQLState()) || "40001".equals(e.getSQLState());
		}

		void backoff() throws InterruptedException {
			// short random backoff: conflicts are only 3 rows wide now, so 1-10 ms is enough to break the tie
			Thread.sleep(ThreadLocalRandom.current().nextInt(1, 11));
		}

		void resetAfterWarmup() throws SQLException {
			if (!con.getAutoCommit()) con.rollback();
			con.setAutoCommit(true);
			con.setReadOnly(false);
			for (PreparedStatement ps : preparedOnce.values()) ps.close();
			preparedOnce.clear();
		}

		@Override
		public void close() throws SQLException {
			con.close();
		}
	}
}
