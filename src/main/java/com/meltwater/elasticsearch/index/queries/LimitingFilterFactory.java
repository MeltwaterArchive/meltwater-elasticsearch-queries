package com.meltwater.elasticsearch.index.queries;

import com.google.common.base.Optional;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermTermsProducer;
import org.apache.lucene.index.TermsProducer;
import org.apache.lucene.index.WildcardPhraseQuery;
import org.apache.lucene.index.WildcardTermsProducer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * The purpose of this class is to create an 'approximation' query for what documents can match a given query, without
 * making the query too expensive to compute.
 *
 * Generating the queries come with more oddities than you would expect.
 *
 * First of, we need to keep track of if the sub-clause that we are currently is has been negated. This is because we in
 * a non-negated query can return more documents than would actually match, but if we have negated it the opposite is
 * true, then we need to be exact.
 *
 * Since we can never be exact for positional queries, this means that such queries can never be used for limiting
 * filtering when in a negation. E.g. (Wildcard)Phrase queries can be used, but only if the number of terms is 1, since
 * they then in effect are not positional queries.
 *
 *
 */

public class LimitingFilterFactory {

    public Optional<Query> limitingFilter(Query query) {
        return limitingFilter(query, false);
    }

    private Optional<Query> limitingFilter(Query query, boolean isNegated) {
        if (query instanceof SpanQuery) {
            return limitingFilterForSpan((SpanQuery) query, isNegated);
        } else if (query instanceof Filter) {
            return Optional.of(query);
        } else if (query instanceof BooleanQuery) {
            return boolQuery((BooleanQuery) query, isNegated);
        } else if (query instanceof TermQuery) {
            return Optional.of(query);
        } else if (query instanceof PhraseQuery) {
            return phraseFilter((PhraseQuery) query, isNegated);
        } else if (query instanceof MultiTermQuery) {
            return Optional.of(query);
        } else if (query instanceof WildcardPhraseQuery) {
            return wildcardPhraseFilter((WildcardPhraseQuery) query, isNegated);
        } else if (query instanceof ToParentBlockJoinQuery) {
            //This can be really bad for performance, if the nested query contains expensive operations (phrases/spans)
            //On the other hand, it is only slow if the field actually has any data, and we currently do not have
            // any data in the only nested text field (enrichments.sentences)
            return Optional.of(query);
        } else {
            //This should never happen, but if it does, it might be really bad for performance
            //logger.warn("failed to limit query, this should never happen. Query : [{}]", query.toString());
            return Optional.of(query);
        }
    }

    private Optional<Query> limitingFilterForSpan(SpanQuery query, boolean isNegated) {
        if (!isNegated) {
            return Optional.of(spanFilter(query));
        } else {
            return Optional.absent();
        }
    }

    private Query spanFilter(SpanQuery query) {
        if (query instanceof SpanNearQuery) {
            return spanNearFilter((SpanNearQuery) query);
        } else if (query instanceof SpanNotQuery) {
            return spanNotFilter((SpanNotQuery) query);
        } else if (query instanceof SpanOrQuery) {
            return spanOrFilter((SpanOrQuery) query);
        } else if (query instanceof SpanTermQuery) {
            return new TermQuery(((SpanTermQuery) query).getTerm());
        } else if (query instanceof SpanMultiTermQueryWrapper) {
            return ((SpanMultiTermQueryWrapper) query).getWrappedQuery();
        } else {
            return new QueryWrapperFilter(query);
        }
    }

    private Optional<Query> boolQuery(BooleanQuery query, boolean isNegated) {
        List<Query> shouldClauses = new ArrayList<>();
        List<Query> mustClauses = new ArrayList<>();
        List<Query> mustNotClauses = new ArrayList<>();
        List<Query> filterClauses = new ArrayList<>();
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
                case FILTER:
                    addIfPresent(filterClauses, limitingFilter(clause.getQuery(), isNegated));
                    break;
                case MUST_NOT:
                    addIfPresent(mustNotClauses, limitingFilter(clause.getQuery(), true));
                    break;
            }
        }
        if(originalMustClauses != mustClauses.size() && isNegated){
            return filterOrAbsent(filterClauses);
        }
        else if (mustClauses.isEmpty() && mustNotClauses.isEmpty() && shouldClauses.isEmpty()) {
            return filterOrAbsent(filterClauses);
        }
        else if (mustClauses.isEmpty() && shouldClauses.isEmpty()) {
            return Optional.of(withFilter(notAny(mustNotClauses), filterClauses));
        }
        else if (mustClauses.isEmpty() && mustNotClauses.isEmpty()) {
            return Optional.of(withFilter(any(shouldClauses), filterClauses));
        }
        else {
            BooleanQuery ret = new BooleanQuery();
            for(Query must:mustClauses){
                ret.add(must, BooleanClause.Occur.MUST);
            }
            for(Query should:shouldClauses){
                ret.add(should, BooleanClause.Occur.SHOULD);
            }
            for(Query mustNot:mustNotClauses){
                ret.add(mustNot, BooleanClause.Occur.MUST_NOT);
            }
            return Optional.of(withFilter(ret, filterClauses));
        }
    }

    private Query withFilter(BooleanQuery bq, List<Query> filterClauses) {
        for(Query fq: filterClauses){
            bq.add(fq, BooleanClause.Occur.FILTER);
        }
        return bq;
    }

    private Optional<Query> filterOrAbsent(List<Query> filterClauses) {
        if(!filterClauses.isEmpty()){
            return Optional.<Query>of(all(filterClauses));
        }
        else{
            return Optional.absent();
        }
    }


    private void addIfPresent(List<Query> clauses, Optional<Query> filter) {
        if (filter.isPresent()) {
            clauses.add(filter.get());
        }
    }

    private static Optional<Query> phraseFilter(PhraseQuery query, boolean isNegated) {
        Term[] terms = query.getTerms();
        if (terms.length == 0) {
            return Optional.absent();
        } else if (terms.length == 1) {
            return Optional.<Query>of(new TermQuery(terms[0]));
        } else if (!isNegated) {
            List<Query> ret = new ArrayList<>();
            for (Term t : terms) {
                ret.add(new TermQuery(t));
            }
            return Optional.<Query>of(all(ret));
        } else {
            return Optional.absent();
        }
    }

    private Optional<Query> wildcardPhraseFilter(WildcardPhraseQuery query, boolean isNegated) {
        if (isNegated && query.getProducers().size() > 1) {
            return Optional.absent();
        }
        List<Query> sub = new ArrayList<>();
        for (TermsProducer prod : query.getProducers()) {
            if (prod instanceof TermTermsProducer) {
                sub.add(new TermQuery(((TermTermsProducer) prod).getTerm()));
            }
        }
        if (sub.isEmpty()) {
            for (TermsProducer prod : query.getProducers()) {
                if (prod instanceof WildcardTermsProducer) {
                    sub.add(new WildcardQuery(((WildcardTermsProducer) prod).getTerm()));
                }
            }
        }
        return Optional.<Query>of(all(sub));
    }

    private Query spanNearFilter(SpanNearQuery query) {
        List<Query> ret = new ArrayList<>();
        for (SpanQuery sub : query.getClauses()) {
            ret.add(spanFilter(sub));
        }
        return all(ret);
    }

    private Query spanNotFilter(SpanNotQuery query) {
        return spanFilter(query.getInclude());
    }

    private Query spanOrFilter(SpanOrQuery query) {
        List<Query> ret = new ArrayList<>();
        for (SpanQuery sub : query.getClauses()) {
            ret.add(spanFilter(sub));
        }
        return any(ret);
    }

    private static BooleanQuery any(List<Query> shouldClauses) {
        return combineWith(shouldClauses, BooleanClause.Occur.SHOULD);
    }

    private static BooleanQuery all(List<Query> shouldClauses) {
        return combineWith(shouldClauses, BooleanClause.Occur.MUST);
    }

    private static BooleanQuery notAny(List<Query> mustNotClauses) {
        return combineWith(mustNotClauses, BooleanClause.Occur.MUST_NOT);
    }

    private static BooleanQuery combineWith(List<Query> clauses, BooleanClause.Occur occur){
        BooleanQuery ret = new BooleanQuery();
        for(Query q:clauses){
            ret.add(q, occur);
        }
        return ret;
    }
}
