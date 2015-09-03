package com.meltwater.elasticsearch.index.queries;

import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.search.spans.*;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.NotFilter;
import org.elasticsearch.common.lucene.search.OrFilter;
import org.elasticsearch.common.lucene.search.XFilteredQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * The purpose of this class is to create an 'approximation' filter for what documents can match a given query, without
 * making the filter too expensive to compute.
 *
 * Generating the filters come with more oddities than you would expect.
 *
 * First of, we need to keep track of if the sub-clause that we are currently is has been negated. This is because we in
 * a non-negated query can return more documents than would actually match, but if we have negated it the opposite is
 * true, then we need to be exact.
 *
 * Since we can never be exact for positional queries, this means that such queries can never be used for limiting
 * filtering when in a negation. E.g. (Wildcard)Phrase queries can be used, but only if the number of terms is 1, since
 * they then in effect are not positional queries.
 *
 * The next big oddity is the {@link BooleanQuery}, which contrary to all logic does not have the same semantics as the
 * {@link org.apache.lucene.queries.BooleanFilter}. Thus, the logic to deal with it is very strange.
 *
 */

public class LimitingFilterFactory {

    public Optional<Filter> limitingFilter(Query query) {
        return limitingFilter(query, false);
    }

    private Optional<Filter> limitingFilter(Query query, boolean isNegated) {
        if (query instanceof SpanQuery) {
            return limitingFilterForSpan((SpanQuery) query, isNegated);
        } else if (query instanceof XFilteredQuery) {
            return xQueryFilter((XFilteredQuery) query, isNegated);
        } else if (query instanceof BooleanQuery) {
            return boolFilter((BooleanQuery) query, isNegated);
        } else if (query instanceof TermQuery) {
            return Optional.<Filter>of(new TermFilter(((TermQuery) query).getTerm()));
        } else if (query instanceof PhraseQuery) {
            return phraseFilter((PhraseQuery) query, isNegated);
        } else if (query instanceof MultiTermQuery) {
            return Optional.<Filter>of(new MultiFilter<>((MultiTermQuery) query));
        } else if (query instanceof WildcardPhraseQuery) {
            return wildcardPhraseFilter((WildcardPhraseQuery) query, isNegated);
        } else if (query instanceof ToParentBlockJoinQuery) {
            //This can be really bad for performance, if the nested query contains expensive operations (phrases/spans)
            //On the other hand, it is only slow if the field actually has any data, and we currently do not have
            // any data in the only nested text field (enrichments.sentences)
            return Optional.<Filter>of(new QueryWrapperFilter(query));
        } else {
            //This should never happen, but if it does, it might be really bad for performance
            //logger.warn("failed to limit query, this should never happen. Query : [{}]", query.toString());
            return Optional.<Filter>of(new QueryWrapperFilter(query));
        }
    }

    private Optional<Filter> limitingFilterForSpan(SpanQuery query, boolean isNegated) {
        if (!isNegated) {
            return Optional.of(spanFilter(query));
        } else {
            return Optional.absent();
        }
    }

    private Filter spanFilter(SpanQuery query) {
        if (query instanceof SpanNearQuery) {
            return spanNearFilter((SpanNearQuery) query);
        } else if (query instanceof SpanNotQuery) {
            return spanNotFilter((SpanNotQuery) query);
        } else if (query instanceof SpanOrQuery) {
            return spanOrFilter((SpanOrQuery) query);
        } else if (query instanceof SpanTermQuery) {
            return new TermFilter(((SpanTermQuery) query).getTerm());
        } else if (query instanceof SpanMultiTermQueryWrapper) {
            return new MultiFilter<>((MultiTermQuery) ((SpanMultiTermQueryWrapper) query).getWrappedQuery());
        } else {
            return new QueryWrapperFilter(query);
        }
    }

    private Optional<Filter> xQueryFilter(XFilteredQuery query, boolean isNegated) {
        Optional<Filter> sub = limitingFilter(query.getQuery(), isNegated);
        if (sub.isPresent()) {
            return Optional.<Filter>of(new AndFilter(Arrays.asList(sub.get(), query.getFilter())));
        } else {
            return Optional.of(query.getFilter());
        }
    }

    private Optional<Filter> boolFilter(BooleanQuery query, boolean isNegated) {
        List<Filter> shouldClauses = new ArrayList<>();
        List<Filter> mustClauses = new ArrayList<>();
        List<Filter> mustNotClauses = new ArrayList<>();
        int originalMustClauses = 0;
        for (BooleanClause clause : query.clauses()) {
            switch (clause.getOccur()) {
                case SHOULD:
                    addIfPresent(shouldClauses, limitingFilter(clause.getQuery(), isNegated));
                    break;
                case MUST:
                    originalMustClauses++;
                    addIfPresent(mustClauses, limitingFilter(clause.getQuery(), isNegated));
                    break;
                case MUST_NOT:
                    addIfPresent(mustNotClauses, limitingFilter(clause.getQuery(), true));
                    break;
            }
        }

        if(originalMustClauses != mustClauses.size() && isNegated){
            return Optional.absent();
        }
        if (mustClauses.isEmpty() && mustNotClauses.isEmpty() && shouldClauses.isEmpty()) {
            return Optional.absent();
        }

        if (mustClauses.isEmpty() && mustNotClauses.isEmpty()) {
            return Optional.<Filter>of(new OrFilter(shouldClauses));
        } else {
            if (mustClauses.isEmpty() && !shouldClauses.isEmpty()) {
                mustClauses.add(new OrFilter(shouldClauses));
            }
            if (!mustNotClauses.isEmpty()) {
                mustClauses.add(new NotFilter(new OrFilter(mustNotClauses)));
            }
            return Optional.<Filter>of(new AndFilter(mustClauses));
        }
    }

    private void addIfPresent(List<Filter> clauses, Optional<Filter> filter) {
        if (filter.isPresent()) {
            clauses.add(filter.get());
        }
    }

    private static Optional<Filter> phraseFilter(PhraseQuery query, boolean isNegated) {
        Term[] terms = query.getTerms();
        if (terms.length == 0) {
            return Optional.absent();
        } else if (terms.length == 1) {
            return Optional.<Filter>of(new TermFilter(terms[0]));
        } else if (!isNegated) {
            List<Filter> ret = new ArrayList<>();
            for (Term t : terms) {
                ret.add(new TermFilter(t));
            }
            return Optional.<Filter>of(new AndFilter(ret));
        } else {
            return Optional.absent();
        }
    }

    private Optional<Filter> wildcardPhraseFilter(WildcardPhraseQuery query, boolean isNegated) {
        if (isNegated && query.getProducers().size() > 1) {
            return Optional.absent();
        }
        List<Filter> sub = new ArrayList<>();
        for (TermsProducer prod : query.getProducers()) {
            if (prod instanceof TermTermsProducer) {
                sub.add(new TermFilter(((TermTermsProducer) prod).getTerm()));
            }
        }
        if (sub.isEmpty()) {
            for (TermsProducer prod : query.getProducers()) {
                if (prod instanceof WildcardTermsProducer) {
                    sub.add(new MultiFilter<>(new WildcardQuery(((WildcardTermsProducer) prod).getTerm())));
                }
            }
        }
        return Optional.<Filter>of(new AndFilter(sub));
    }

    private Filter spanNearFilter(SpanNearQuery query) {
        List<Filter> ret = new ArrayList<>();
        for (SpanQuery sub : query.getClauses()) {
            ret.add(spanFilter(sub));
        }
        return new AndFilter(ret);
    }

    private Filter spanNotFilter(SpanNotQuery query) {
        return spanFilter(query.getInclude());
    }

    private Filter spanOrFilter(SpanOrQuery query) {
        List<Filter> ret = new ArrayList<>();
        for (SpanQuery sub : query.getClauses()) {
            ret.add(spanFilter(sub));
        }
        return new OrFilter(ret);
    }


    /**
     * Subclass to get around protected constructor of superclass.
     */

    static class MultiFilter<Q extends MultiTermQuery> extends MultiTermQueryWrapperFilter<Q> {
        protected MultiFilter(Q query) {
            super(query);
        }
    }

}
