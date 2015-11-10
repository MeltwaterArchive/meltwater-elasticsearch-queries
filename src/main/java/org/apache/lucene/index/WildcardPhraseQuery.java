package org.apache.lucene.index;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Query implementation that tries to create a
 * {@link MultiPhraseQuery} from inserted
 * {@link org.apache.lucene.index.TermsProducer}s.
 *
 * Each term producer is responsible for producing the terms that are allowed
 * to be in that position. The order of the term producers are maintained to mean
 * the ordering in the value.
 */

public class WildcardPhraseQuery extends Query {
    private static final MatchNoDocsQuery matchNoDocsQuery = new MatchNoDocsQuery();

    private LinkedList<TermsProducer> producers = new LinkedList<>();
    private LinkedList<Integer> positions = new LinkedList<>();

    public void add(TermsProducer producer, int position){
        producers.add(producer);
        positions.add(position);
    }

    public LinkedList<TermsProducer> getProducers() {
        return producers;
    }

    public LinkedList<Integer> getPositions() {
        return positions;
    }

    @Override
    public String toString(String field) {
        StringBuilder builder = new StringBuilder("WildcardPhrase(");
        for(int i = 0; i< producers.size(); i++){
            if(i > 0) {
                builder.append(", ");
            }
            builder.append(producers.get(i).toString());
        }
        return builder.append(")").toString();
    }

    public Query rewrite(IndexReader reader) throws IOException {
        MultiPhraseQuery multi = new MultiPhraseQuery();
        for (int i = 0; i < producers.size(); i++) {
            Term[] terms = producers.get(i).getTerms(reader);
            if (terms.length < 1) {
                return matchNoDocsQuery;
            }
            multi.add(terms, positions.get(i));
        }
        return multi.rewrite(reader);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        WildcardPhraseQuery that = (WildcardPhraseQuery) o;

        if (producers != null ? !producers.equals(that.producers) : that.producers != null) return false;
        return !(positions != null ? !positions.equals(that.positions) : that.positions != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (producers != null ? producers.hashCode() : 0);
        result = 31 * result + (positions != null ? positions.hashCode() : 0);
        return result;
    }
}
