package distributedHBP;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class Vertex {

	public int id;
	public TIntHashSet ids;
	public double hotness;
	public TIntDoubleHashMap neighbor;

	public Vertex(int id, double hotness, TIntDoubleHashMap neighbor) {
		this.id = id;
		this.hotness = hotness;
		this.neighbor = neighbor;
		ids = new TIntHashSet();
		ids.add(id);
	}

	public void merge(Vertex v) {
		ids.add(v.id);
		hotness += v.hotness;
		TIntDoubleHashMap vneighbor = v.neighbor;
		TIntDoubleIterator itr = vneighbor.iterator();
		while (itr.hasNext()) {
			itr.advance();
			int key = itr.key();
			if (neighbor.contains(key)) {
				neighbor.put(key, neighbor.get(key) + itr.value());
			}else{
				neighbor.put(key, itr.value());
			}
		}
		v.neighbor.clear();
	}

}
