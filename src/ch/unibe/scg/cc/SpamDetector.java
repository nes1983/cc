package ch.unibe.scg.cc;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;

/**
 * A detector that can detect spammy clone results. Give it two strings, and the
 * detector will tell you if they're not the kind of result a user would want to
 * look at. Otherwise, it is considered <em>spammy</em>.
 *
 * @author nes
 */

// TODO(niko): Use all features from doi://10.1109/CSMR.2012.37
public final class SpamDetector {
    final Pattern identifier = Pattern.compile("\\p{Alpha}\\p{Alnum}*");

    /** Feature vector for classifying a cloning result as spam or not */
    public final static class FeatureVector {
        final double vocabularySimilarity;

        FeatureVector(double vocabularySimilarity) {
            this.vocabularySimilarity = vocabularySimilarity;
        }

        @Override
        public String toString() {
            return "FeatureVector[vocabularySimilarity=" + vocabularySimilarity + "]";
        }
    }

    /**
     * @return the feature vector that decides whether or not the clone
     *         represented by doc1 and doc2 is spammy.
     */
    public FeatureVector extractFeatureVector(String doc1, String doc2) {
        Set<String> v1 = extractVocabulary(doc1);
        Set<String> v2 = extractVocabulary(doc2);

        double vocabularySimilarity = ((double) Sets.intersection(v1, v2).size())
                / ((double) Sets.union(v1, v2).size());

        return new FeatureVector(vocabularySimilarity);
    }

    /** @return whether {@code v} represents a cloning result that is spammy. */
    public boolean isSpamByParameters(FeatureVector v) {
        // A low vocabulary similarity suggests that everything was renamed.
        // That's unlikely, so probably we're looking at something that wasn't
        // cloned at all.
        return v.vocabularySimilarity < 0.8;
    }

    /**
     * Get all identifiers from the document. To speed up the operation, the set
     * may include more than the identifiers.
     */
    Set<String> extractVocabulary(String doc) {
        Set<String> ret = Sets.newHashSet();
        Matcher match = identifier.matcher(doc);
        while (match.find()) {
            ret.add(match.group());
        }
        return ret;
    }
}
