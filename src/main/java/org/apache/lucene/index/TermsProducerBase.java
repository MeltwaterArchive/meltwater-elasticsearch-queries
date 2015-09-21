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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TermsProducerBase that = (TermsProducerBase) o;

        return !(term != null ? !term.equals(that.term) : that.term != null);

    }

    @Override
    public int hashCode() {
        return term != null ? term.hashCode() : 0;
    }

}
