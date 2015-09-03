package org.apache.lucene.index;

import java.io.IOException;


public interface TermsProducer {
    public Term[] getTerms(IndexReader reader) throws IOException;

}


