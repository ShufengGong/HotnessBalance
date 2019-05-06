package distributedHBP;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AssignMiniPart {

	public static class ASNMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		private int vnum;
		private int pnum;
		private int range;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			vnum = conf.getInt("vnum", -1);
			pnum = conf.getInt("pnum", -1);
			range = vnum / pnum + 1;
			if (vnum == -1 || pnum == -1) {
				System.err.println("invalid vnum != pnum");
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer stk = new StringTokenizer(line);
			int id = Integer.parseInt(stk.nextToken());
			int prtId = id / range;
			context.write(new IntWritable(prtId), value);
		}
	}

	public static class ASNReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		private int k_;
		private int range;
		TIntHashSet[] parts;
		double alpha;
		double gamma;
		private MultipleOutputs mos;

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			k_ = conf.getInt("k_", -1);
			alpha = conf.getFloat("alpha", -1);
			gamma = conf.getFloat("gamma", -1);
			if (k_ == -1 || gamma == -1 || alpha == -1) {
				System.err.println("invalid parameter");
			}
			parts = new TIntHashSet[k_];
			mos = new MultipleOutputs(context);
		}

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int partId = key.get();
			ArrayList<Vertex> listH = new ArrayList<Vertex>();
			ArrayList<Vertex> listC = new ArrayList<Vertex>();

			double hotsumH = 0;
			double hotsumC = 0;

			for (Text value : values) {
				StringTokenizer stk = new StringTokenizer(value.toString());
				String flag = stk.nextToken();
				stk.nextToken(); // skip virtual id that for current hash
									// partition
				int id = Integer.parseInt(stk.nextToken());
				double hot = Double.parseDouble(stk.nextToken());
				TIntDoubleHashMap neighbor = new TIntDoubleHashMap();
				while (stk.hasMoreTokens()) {
					int nid = Integer.parseInt(stk.nextToken());
					double comm = Double.parseDouble(stk.nextToken());
					neighbor.put(nid, comm);
				}
				Vertex vertex = new Vertex(id, hot, neighbor);
				if (flag.equals("h")) {
					listH.add(vertex);
					hotsumH += hot;
				} else {
					listC.add(vertex);
					hotsumC += hot;
				}
			}

			assign(partId, hotsumH, listH, mos, "H");
			assign(partId, hotsumC, listC, mos, "C");

		}

		public void assign(int partId, double hotsum, ArrayList<Vertex> list,
				MultipleOutputs mos, String flag) throws IOException,
				InterruptedException {
			double v = 1.1;
			double miu = v * hotsum / k_;
			double[] hotness = new double[k_];
			int size = list.size();
			for (int j = 0; j < size; j++) {
				Vertex vertex = list.get(j);
				double hot = vertex.hotness;
				TIntDoubleHashMap neighbor = vertex.neighbor;
				double score = 0;
				double maxScore = -Double.MAX_VALUE;
				int maxPart = -1;
				for (int i = 0; i < k_; i++) {
					if (hotness[i] <= miu) {
						score = getScore(neighbor, parts[i], hotness[i], hot,
								alpha, gamma);
						if (maxScore < score) {
							maxScore = score;
							maxPart = i;
						}
					}
					// System.out.print(score + " ");
				}
				// System.out.println();
				if (maxPart == -1) {
					System.out.println("there is no insert!!!");
					System.exit(0);
				}
				parts[maxPart].add(vertex.id);
				hotness[maxPart] += hot;
				if (flag.equals("H")) {
					mos.write("miniPartInfo", new IntWritable(vertex.id),
							new Text((partId * 2 * k_ + maxPart) + ""));
				} else {
					mos.write("miniPartInfo", new IntWritable(vertex.id),
							new Text((partId * 2 * k_ + maxPart + k_) + ""));
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		}

		public double getScore(TIntDoubleHashMap neighbor,
				TIntHashSet partition, double partHotness, double hotness,
				double alpha, double gamma) {
			double score = 0;
			TIntDoubleIterator itr = neighbor.iterator();
			while (itr.hasNext()) {
				itr.advance();
				int id = itr.key();
				if (partition.contains(id)) {
					score += itr.value();
				}
			}

			double deltaC = alpha
					* (Math.pow(partHotness + hotness, gamma) - Math.pow(
							partHotness, gamma));

			return score - deltaC;

		}
	}

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		String input = null;
		String output = null;
		int vnum = 0;
		int pnum = 0;
		int rnum = 0;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-in")) {
				input = args[++i];
			}
			if (args[i].equals("-out")) {
				output = args[++i];
			}
			if (args[i].equals("-pnum")) {
				pnum = Integer.parseInt(args[++i]);
			}
			if (args[i].equals("-vnum")) {
				vnum = Integer.parseInt(args[++i]);
			}
			if (args[i].equals("-rnum")) {
				rnum = Integer.parseInt(args[++i]);
			}
		}

		if (vnum == 0 || pnum == 0 || rnum == 0) {
			System.err
					.println("invalid parameters =======================================");
			return;
		}
		if (input == null || output == null) {
			System.err
					.println("invalid parameters =======================================");
			return;
		}

		System.out.println("start +++++++++++++++++++");

		conf.setInt("vnum", vnum);
		conf.setInt("pnum", pnum);
		try {
			Job job = new Job(conf, "DHBP");
			job.setJarByClass(AssignMiniPart.class);
			job.setMapperClass(ASNMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(ASNReducer.class);
			job.setNumReduceTasks(rnum);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
