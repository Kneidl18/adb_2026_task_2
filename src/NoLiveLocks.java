import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 *
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
*/

public class NoLiveLocks {
	public static int num_taks=30;
	public static int num_queries_per_task = 3;
	/** after this many aborts a transaction runs exclusively, which bounds the number of restarts (no starvation) */
	public static int max_retries = 3;

	Connection con;
	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	AtomicInteger restarts = new AtomicInteger(0);
	AtomicInteger escalations = new AtomicInteger(0);

	public NoLiveLocks(){
		tasks_finished = new AtomicInteger(0);
	}

	void executeSQL(String sql, Connection con){
		java.sql.Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			System.out.println("Executing "+sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	void cerateDB(){
		try {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);

			PreparedStatement ps;
			con.setAutoCommit(false);
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

			for(int i=0;i<num_tuple;i++){
				ps.setInt(1, i);
				ps.setDouble(2,rand.nextDouble()*max_value);
				ps.setInt(3, 0);
				ps.addBatch();
				ps.clearParameters();
			}

			int[] results = ps.executeBatch();
			con.commit();
			con.close();
			System.out.println(Arrays.toString(results));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args){
		//Create DB
		NoLiveLocks b = new NoLiveLocks();
		b.cerateDB();

		//Create task and their queries
		ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<NoLiveLocks.Blocker>(num_taks);
		for(int task=0;task<num_taks;task++){
			my_tasks.add(b.new Blocker(task));
		}

		//Execute tasks
		ThreadPoolExecutor executor =
				  (ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
		double start, stop;
		start = System.currentTimeMillis();
		for(Blocker task : my_tasks) {
			executor.execute(task);
		}
		executor.shutdown();

		//wait for the tasks to finish
		while(!executor.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		stop = System.currentTimeMillis();
		System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks
				+", restarts: "+b.restarts+", exclusive runs: "+b.escalations);
	}

	/**
	 * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
	 *
	 */
	public class Blocker implements Runnable  {
		final int task;
		final double [] start_range;
		final double [] stop_range;

		public Blocker(int id){
			this.task = id;
			this.start_range = new double[num_queries_per_task];
			this.stop_range  = new double[num_queries_per_task];
			for(int q=0;q<num_queries_per_task;q++) {
				stop_range[q]  = rand.nextDouble()*max_value;
				start_range[q] = rand.nextDouble()*stop_range[q];
			}
		}

		@Override
		public void run() {
			Connection con=null;//XXX - Yes there needs to be a separate Connection per blocking taks. Otherwise strange things can happen
			con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);//XXX This is the class you need to create, but does not have to be in the Repo
			int attempts = 0;

			try {
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

				while (true) {//restart the whole transaction until it commits
					attempts++;
					boolean exclusive = attempts > max_retries;
					try (Statement st = con.createStatement();
						 PreparedStatement lock = con.prepareStatement(lockAllRowsSql());
						 PreparedStatement ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?,  modified_by = "+task+" WHERE data_value >= ? AND data_value <= ?;",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
						if (exclusive) {
							//starvation guard: wait until all running transactions on the table are done and keep new ones out.
							//we hold no other lock while waiting, so this cannot be part of a deadlock cycle
							st.execute("LOCK TABLE blocking_data IN EXCLUSIVE MODE");
							escalations.incrementAndGet();
						} else {
							//take the table-level lock the UPDATEs need right away, so it never has to be upgraded later
							st.execute("LOCK TABLE blocking_data IN ROW EXCLUSIVE MODE");
							//deadlock prevention: lock every row of all ranges up front, all transactions in the same global order
							for (int q = 0; q < num_queries_per_task; q++) {
								lock.setDouble(2*q+1, start_range[q]);
								lock.setDouble(2*q+2, stop_range[q]);
							}
							lock.executeQuery().close();
						}

						for (int q = 0; q < num_queries_per_task; q++) {//I want to execute num_queries_per_task update queries
							ps.setDouble(1, (float) (rand.nextDouble()*max_value));//some value, which one is not important.
							ps.setDouble(2, start_range[q]);
							ps.setDouble(3, stop_range[q]);
							ps.execute();
						}
						con.commit();
						break;
					} catch (SQLException e) {
						//the aborted transaction stays open until we roll back, otherwise every retry fails with "current transaction is aborted"
						con.rollback();
						//40P01 = deadlock detected -> try again
						if (! "40P01".equals(e.getSQLState())) {
							throw e;
						}
						restarts.incrementAndGet();
						System.out.println("Task "+task+" aborted in attempt "+attempts+" ("+e.getSQLState()+"), restarting");
						Thread.sleep(ThreadLocalRandom.current().nextInt(50, 200));//random backoff, so the same transactions do not collide again immediately
					}
				}

				int num_finished = tasks_finished.incrementAndGet();
				System.out.println("Done task "+task+" after "+attempts+" attempt(s). I am finisher number "+num_finished);
			} catch (SQLException e) {
				System.err.println("Task "+task+" failed: "+e.getMessage());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		/** SELECT ... FOR UPDATE over the union of all ranges of this task, rows locked in data_id order */
		String lockAllRowsSql() {
			StringBuilder sb = new StringBuilder("SELECT data_id FROM blocking_data WHERE ");
			for (int q = 0; q < num_queries_per_task; q++) {
				sb.append(q == 0 ? "" : " OR ").append("(data_value >= ? AND data_value <= ?)");
			}
			return sb.append(" ORDER BY data_id FOR UPDATE").toString();
		}
	}
}
