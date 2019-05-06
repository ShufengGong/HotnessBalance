package distributedHBP;

import gnu.trove.iterator.TIntDoubleIterator;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class Vertex {

	public int id;
	public double hotness;
	public TIntDoubleHashMap neighbor;

	public Vertex(int id, double hotness, TIntDoubleHashMap neighbor) {
		this.id = id;
		this.hotness = hotness;
		this.neighbor = neighbor;
	}
}
