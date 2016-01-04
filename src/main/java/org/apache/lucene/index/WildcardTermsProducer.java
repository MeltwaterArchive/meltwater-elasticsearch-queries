package org.apache.lucene.index;

import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link org.apache.lucene.index.TermsProducer}
 * implementation that resolves all the possible values for the specified
 * term, where the term value may be a wildcard (as defined in
 * {@link WildcardQuery}.
 */
public class WildcardTermsProducer extends TermsProducerBase {

    public WildcardTermsProducer(Term term) {
        super(term);
    }

    private static Term[] EMPTY = new Term[]{};

    @Override
    public Term[] getTerms(IndexReader reader) throws IOException {
        TermsEnum automatonTermsEnum = wildcardEnumeration(reader);
        return automatonTermsEnum != null? allTerms(automatonTermsEnum):EMPTY;
    }

    private Term[] allTerms(TermsEnum automatonTermsEnum)
            throws IOException {
        Set<Term> ret = new HashSet<>();
        BytesRef ref;
        while ((ref = automatonTermsEnum.next()) != null) {
            ret.add(new Term(term.field(), BytesRef.deepCopyOf(ref)));

        }
        return ret.toArray(new Term[ret.size()]);
    }

    private TermsEnum wildcardEnumeration(final IndexReader reader)
            throws IOException {
        Terms terms = MultiFields.getTerms(reader, term.field());
        if(terms == null){
            return null;
        }
        return new AutomatonTermsEnum(
                terms.iterator(),
                new CompiledAutomaton(
                        WildcardQuery.toAutomaton(term),
                        false, false));
    }
}
