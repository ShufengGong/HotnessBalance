package distributedHBP;

import gnu.trove.set.hash.TIntHashSet;

public class SuperVertex {
	
	public TIntHashSet ids;
	public double hotness;
	
	public SuperVertex(int id, double hotness){
		ids = new TIntHashSet();
		ids.add(id);
		this.hotness = hotness;
	}
	
	public void merge(SuperVertex sv){
		ids.addAll(sv.ids);
		hotness += sv.hotness;
	}
	
	public void add(int id){
		ids.add(id);
	}

}
