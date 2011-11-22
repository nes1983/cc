package ch.unibe.scg.cc.lines;


public class LineBreak {
	int position;
	int weight;

	LineBreak(int position, int weight) {
		this.position = position;
		this.weight = weight;
	}
	
	public String toString() {
		return "<" + position + "," + weight + ">";
	}
	
	public int getPosition() {
		return position;
	}
	
	public int getWeight() {
		return weight;
	}
}