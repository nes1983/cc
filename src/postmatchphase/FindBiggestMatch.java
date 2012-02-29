//package postmatchphase;
//
//import java.io.IOException;
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Set;
//
//import javax.inject.Inject;
//
//import org.apache.commons.collections.CollectionUtils;
//import org.apache.commons.collections.Transformer;
//
//import ch.unibe.scg.cc.activerecord.HashFact;
//import ch.unibe.scg.cc.activerecord.Location;
//
//import ch.unibe.scg.cc.lines.ModifiableLines;
//import ch.unibe.scg.cc.lines.StringOfLines;
//
//public class FindBiggestMatch {
//	
//	@Inject
//	HashFactLoader hashFactLoader;
//
//	public void findBiggestMatch(Collection<HashFact> facts) {
//		Transformer toModifiableLines = new Transformer() {
//			public Object transform(Object arg) {
//				HashFact fact = (HashFact) arg;
//				StringOfLines lines = hashFactLoader.load(fact);
//				return lines;
//			}
//		};
//		
//		@SuppressWarnings("unchecked")
//		Collection<StringOfLines> lines = CollectionUtils.collect(facts,  toModifiableLines);
//		
//		escalateUpAndDown(facts, lines);
//
//	}
//
//	void escalateUpAndDown(List<HashFact> facts,
//			List<StringOfLines> lines) {
//		Map<HashFact, Location> currentMatches = makeMatch(facts);
//		escalateUp(currentMatches);
//		
//	}
//
//	boolean canEscalateUp(Map<HashFact, Location> currentMatches) {
//		Set<String> all = new HashSet<String>();
//		for(Entry<HashFact, Location> entry : currentMatches.entrySet()) {
//			HashFact fact = entry.getKey();
//			StringOfLines file = loadFact(fact);
//			all.add(file.getLines(fact.getLocation().getFirstLine()-1, fact.getLocation().getLength()+1));
//		}
//		return type3Clone(all);
//		
//	}
//
//	StringOfLines loadFact(HashFact fact) {
//		StringOfLines file;
//		try {
//			file = hashFactLoader.loadFile(fact);
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//		return file;
//	}
//
//	IterableMap<HashFact, Location> makeMatch(List<HashFact> facts) {
//		IterableMap<HashFact, Location> matches = new LinkedMap<HashFact, Location>();
//		for(int i=0;i<facts.size();i++) {
//			matches.put(facts.get(i), facts.get(i).getLocation());
//		}
//		return matches;
//	}
//	
//}
//
