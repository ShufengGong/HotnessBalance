package distributedHBP;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Compress {

	public static class CMPMapper extends
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

	public static class CMPReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		private int vnumH;
		private int vnumC;
		private int pnum;
		private int k_;
		private int[] vidToSVid;
		private MultipleOutputs mos;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			vnumH = conf.getInt("vnumH", -1);
			vnumC = conf.getInt("vnumC", -1);
			k_ = conf.getInt("k_", -1);
			if (vnumH == -1 || vnumC == -1 || k_ == -1) {
				System.err.println("invalid parameter");
			}
			vidToSVid = new int[vnumH];
			Path cachePath[] = DistributedCache.getLocalCacheFiles(conf);
			if (cachePath != null && cachePath.length > 0) {
				BufferedReader brH = new BufferedReader(new FileReader(
						cachePath[0].toString()));
				String line;
				while ((line = brH.readLine()) != null) {
					String[] ss = line.split("\\s+");
					int vid = Integer.parseInt(ss[0]);
					int svid = Integer.parseInt(ss[1]);
					vidToSVid[vid] = svid;
				}
			}
			mos = new MultipleOutputs(context);
		}

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int partId = key.get();
			Vertex[] listH = new Vertex[k_];
			Vertex[] listC = new Vertex[k_];
			for(int i = 0; i < k_; i++){
				listH[i] = new Vertex(partId * 2 * k_ + i, 0, new TIntDoubleHashMap());
				listC[i] = new Vertex(partId * 2 * k_ + i + k_, 0, new TIntDoubleHashMap());
			}

			for (Text value : values) {
				StringTokenizer stk = new StringTokenizer(value.toString());
				String flag = stk.nextToken();
				stk.nextToken(); // skip virtual id that for current hash
									// partition
				int id = Integer.parseInt(stk.nextToken());
				double hot = Double.parseDouble(stk.nextToken());
				if (flag.equals("H")) {
					int svid = vidToSVid[id];
					int index = svid-2 * k_ * partId;
					listH[index].hotness += hot;
					while (stk.hasMoreTokens()) {
						int nid = Integer.parseInt(stk.nextToken());
						double comm = Double.parseDouble(stk.nextToken());
						int nsvid = vidToSVid[nid];
						if(listH[index].neighbor.containsKey(nsvid)){
							listH[index].neighbor.put(nsvid, listH[index].neighbor.get(nsvid) + comm);
						}
					}
				} else {
					int svid = vidToSVid[id];
					int index = svid-2 * k_ * partId - k_;
					listC[index].hotness += hot;
					while (stk.hasMoreTokens()) {
						int nid = Integer.parseInt(stk.nextToken());
						double comm = Double.parseDouble(stk.nextToken());
						int nsvid = vidToSVid[nid];
						if(listC[index].neighbor.containsKey(nsvid)){
							listC[index].neighbor.put(nsvid, listC[index].neighbor.get(nsvid) + comm);
						}
					}
				}
			}
			for(int i = 0; i < listH.length; i++){
				String line = "";
				TIntDoubleHashMap neighbor = listH[i].neighbor;
				TIntDoubleIterator itr = neighbor.iterator();
				while(itr.hasNext()){
					itr.advance();
					line += itr.key() + " " + itr.value() + " ";
				}
				mos.write("superhot", new IntWritable(listH[i].id),
							new Text(line));
			}
			
			for(int i = 0; i < listC.length; i++){
				String line = "";
				TIntDoubleHashMap neighbor = listC[i].neighbor;
				TIntDoubleIterator itr = neighbor.iterator();
				while(itr.hasNext()){
					itr.advance();
					line += itr.key() + " " + itr.value() + " ";
				}
				mos.write("supercool", new IntWritable(listC[i].id),
							new Text(line));
			}
		}

		@Override
		public void cleanup(Context context) throws IOException {

		}
	}

}
