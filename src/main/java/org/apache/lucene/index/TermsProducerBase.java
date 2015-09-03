package org.apache.lucene.index;


public abstract class TermsProducerBase implements TermsProducer {

    protected final Term term;

    public TermsProducerBase(Term term) {
        this.term = term;
    }

    @Override
    public String toString(){
        return term.toString();
    }

    public Term getTerm(){
        return term;
    }

}
