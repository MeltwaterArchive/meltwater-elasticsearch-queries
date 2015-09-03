package org.apache.lucene.index;

/**
 * {@link org.apache.lucene.index.TermsProducer}
 * implementation that always returns an array of length 1, with the
 * specified as it's only element. Useful for value elements that are terms
 * in the index.
 */
public class TermTermsProducer extends TermsProducerBase{

    public TermTermsProducer(Term term) {
        super(term);
    }

    @Override
    public Term[] getTerms(IndexReader reader) {
        return new Term[]{term};
    }

}
