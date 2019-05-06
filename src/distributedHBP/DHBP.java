package distributedHBP;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.net.URI;
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

public class DHBP {

	public static class HBPMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		
		private int vnum;
		private int pnum;
		private int range;
		
		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			vnum = conf.getInt("vnum", -1);
			pnum = conf.getInt("pnum", -1);
			range = vnum/pnum + 1;
			if(vnum == -1 || pnum == -1){
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

	public static class HBPReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		
		private int vnum;
		private int pnum;
		private int range;
		private double commThres;

		@Override
		protected void setup(Context context) {
			Configuration conf = context.getConfiguration();
			vnum = conf.getInt("vnum", -1);
			pnum = conf.getInt("pnum", -1);
			range = vnum/pnum + 1;
			if(vnum == -1 || pnum == -1){
				System.err.println("invalid vnum != pnum");
			}
		}

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int prtId = key.get();
			Vertex[] vertex = new Vertex[range];
			SuperVertex[] superVertex = new SuperVertex[range];
			
			for (Text value : values) {
				StringTokenizer stk = new StringTokenizer(value.toString());
				int id = Integer.parseInt(stk.nextToken());
				int idIndex = id % range;
				double hotness = Double.parseDouble(stk.nextToken());
				TIntDoubleHashMap neighbor = new TIntDoubleHashMap();
				while(stk.hasMoreTokens()){
					int nid = Integer.parseInt(stk.nextToken());
					int nidIndex = nid % range;
					double comm = Double.parseDouble(stk.nextToken());
					neighbor.put(nid, comm);
					if(comm > commThres){
						if(nid / range == prtId){
							if(superVertex[idIndex] == superVertex[nidIndex]){
								if(superVertex[idIndex] == null){
									superVertex[idIndex] = new SuperVertex(id, hotness);
									superVertex[nidIndex] = superVertex[idIndex];
									superVertex[nidIndex].add(nid);
								}
							}else{
								if(superVertex[idIndex] == null){
									superVertex[idIndex] = superVertex[nidIndex];
									superVertex[idIndex].add(id);
								}else if(superVertex[nidIndex] == null){
									superVertex[nidIndex] = superVertex[idIndex];
									superVertex[nidIndex].add(id);
								}else{
									TIntIterator itr = superVertex[nidIndex].ids.iterator();
									while(itr.hasNext()){
										int iid = itr.next();
										superVertex[iid % range] = superVertex[idIndex];
									}
									superVertex[idIndex].merge(superVertex[nidIndex]);
									superVertex[nidIndex] = superVertex[idIndex];
								}
							}
						}
					}
				}
				vertex[id % range] = new Vertex(id, hotness, neighbor);
			}
			for(int i = 0; i < range; i++){
				Vertex v = vertex[i];
				if(v != null){
					SuperVertex sv = superVertex[i];
					if(sv != null){
						TIntHashSet ids = sv.ids;
						TIntIterator itr = ids.iterator();
						while(itr.hasNext()){
							int id = itr.next();
							if(id % range != i){
								v.merge(vertex[id % range]);
								vertex[id % range] = null;
							}
						}
					}
				}
			}
			
			for(int i = 0; i < range; i++){
				Vertex v = vertex[i];
				if(v != null){
					String s = v.id + " ";
					for(int id : v.ids._set){
						s += id + " ";
					}
				}
			}
			
		}
		

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

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
			if(args[i].equals("-rnum")){
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
			job.setJarByClass(DHBP.class);
			job.setMapperClass(HBPMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(HBPReducer.class);
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
