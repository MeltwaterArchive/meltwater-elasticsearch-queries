package com.meltwater.elasticsearch.index.queries

import com.google.common.base.Joiner
import com.google.common.collect.Sets
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.Field
import org.apache.lucene.document.TextField
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.IndexableField
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.Term
import org.apache.lucene.index.TermTermsProducer
import org.apache.lucene.index.WildcardPhraseQuery
import org.apache.lucene.index.WildcardTermsProducer
import org.apache.lucene.search.BooleanClause
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.PhraseQuery
import org.apache.lucene.search.Query
import org.apache.lucene.search.Scorer
import org.apache.lucene.search.SimpleCollector
import org.apache.lucene.search.TermQuery
import org.apache.lucene.search.WildcardQuery
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper
import org.apache.lucene.search.spans.SpanNotQuery
import org.apache.lucene.search.spans.SpanOrQuery
import org.apache.lucene.search.spans.SpanQuery
import org.apache.lucene.search.spans.SpanTermQuery
import org.apache.lucene.store.Directory
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.util.Version
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll


/**
 * Tests that the LimitingFilter matches the same documents and possibly some extra documents as the original query.
 */
class LimitingFilterFactoryTest extends Specification {

    @Shared
    DataGenerator generator

    @Shared
    Directory dir

    @Shared
    IndexReader reader

    @Shared
    IndexSearcher searcher

    @Shared
    Iterable<Query> queryGenerator

    @Shared
    LimitingFilterFactory factory

    static String nameField = 'name'

    def setupSpec() {
        long seed = 158556155086072256//new Random().nextLong()
        factory = new LimitingFilterFactory()
        println "Running randomized pre-filtering query test with seed: $seed";
        Random r = new Random(seed)
        generator = new DataGenerator(r)
        index(r, generator)
        reader = DirectoryReader.open(dir)
        searcher = new IndexSearcher(reader)
        queryGenerator = new QueriesGenerator(new QueryGenerator(r, generator, 5, 3), 10_000)
    }

    def cleanupSpec() {
        reader.close()
    }

    @Unroll("Has correct hits for #query")
    def testHasSameOrMoreHitsWithFilter() {
        given:
            def filter = factory.limitingFilter(query)
        expect:
            !filter.isPresent() || assertSameOrMoreWithFilter(query, filter.get())
        where:
        query << queryGenerator
    }

    boolean assertSameOrMoreWithFilter(Query q, Query filter) {
        if (!sameOrMoreResults(q,filter)) {
            throwMinDiff(q,filter)
            return false
        } else {
            return true
        }
    }


    boolean sameOrMoreResults(Query q, Query filter) {
        return search(filtered(q, filter)).containsAll(search(q))
    }

    private BooleanQuery filtered(Query q, Query filter) {
        def bq = new BooleanQuery()
        bq.add(q, BooleanClause.Occur.MUST);
        bq.add(filter, BooleanClause.Occur.MUST);
        bq
    }

    Set<Integer> search(Query q) {
        def collector = new DocIdCollector()
        searcher.search(q, collector)
        collector.res
    }

    void index(Random r, DataGenerator dataGen) {
        dir = new RAMDirectory()
        def writer = new IndexWriter(dir, new IndexWriterConfig(new WhitespaceAnalyzer()))
        (0..<1000).each {
            writer.addDocument(dataGen.genDoc())
            if (r.nextBoolean()) {
                writer.commit()
            }
        }
        writer.close()
    }

    def throwMinDiff(Query q, Query filter) {
        if (q instanceof BooleanQuery) {
            BooleanQuery bq = q;
            bq.each {
                def fi = factory.limitingFilter(it.query)
                if(fi.present){
                    if (!sameOrMoreResults(it.query, fi.get())) {
                        throwMinDiff(it.query, fi.get())
                    }
                }
            }
        }
        Set<Integer> matchedWithoutFilter = search(q)
        Set<Integer> matchedWithFilter = search(filter)
        if (!matchedWithFilter.containsAll(matchedWithoutFilter)) {
            throw new AssertionError(buildAssertionMessage(q, filter, matchedWithoutFilter, matchedWithFilter))
        }
    }

    private String buildAssertionMessage(Query q, Query f, Set<Integer> matchedWithoutFilter, Set<Integer> matchedWithFilter) {
        Set<Integer> unwantedDocs = Sets.difference(matchedWithFilter, matchedWithoutFilter)
        Set<Integer> missingDocs = Sets.difference(matchedWithoutFilter, matchedWithFilter)
        String msg = "Filter did not match all documents matched by the query:\nquery: $q\nfilter: $f"
        if (!unwantedDocs.empty) {
            msg += "\n\nMatched docmuents that should not be matched:\n${formatDocs(unwantedDocs)}"
        }
        if (!missingDocs.empty) {
            msg += "\n\nUnmatched docmuents that should be matched:\n${formatDocs(missingDocs)}"
        }
        return msg
    }

    String formatDocs(Collection<Integer> docs) {
        StringBuilder sb = new StringBuilder()
        docs.take(10).each {
            reader.document(it).fields.each {
                sb.append("${it.name()}: ${it.stringValue()}")
                sb.append("\n")
            }
        }
        return sb.toString();
    }


    static <T> T randomElement(Random r, List<T> list) {
        list[r.nextInt(list.size())]
    }

    private class QueryGenerator {

        private Random r
        private DataGenerator dataGen

        int maxDepth
        int maxClauses

        public QueryGenerator(Random r, DataGenerator dataGen, int maxDepth, int maxClauses) {
            this.r = r
            this.dataGen = dataGen
            this.maxDepth = maxDepth
            this.maxClauses = maxClauses
        }

        def termGenerator = { new TermQuery(randomTerm()) }

        def wildcardGenerator = { new WildcardQuery(randomWildcard()) }

        def phraseGenerator = {
            new PhraseQuery().with {
                q ->
                    maxClauses.times { q.add(randomTerm()) }
                    return q
            }
        }

        def wildcardPhraseGenerator = {
            new WildcardPhraseQuery().with {
                q ->
                    maxClauses.times {
                        int pos ->
                            q.add(
                                    r.nextBoolean() ?
                                            new WildcardTermsProducer(randomWildcard()) :
                                            new TermTermsProducer(randomTerm()),
                                    pos)
                    }
                    return q
            }
        }

        def booleanQueryGenerator = {
            int depth ->
                int clauses = r.nextInt(maxClauses - 1) + 1
                def ret = new BooleanQuery()
                ret.add(genQuery(depth + 1), randomElement(r, [BooleanClause.Occur.MUST, BooleanClause.Occur.SHOULD, BooleanClause.Occur.FILTER]))
                (1..<clauses).each {
                    ret.add(genQuery(depth + 1), randomElement(r, Arrays.asList(BooleanClause.Occur.values())))
                }
                ret
        }

        def spanNearGenerator = {
            int depth ->
                new SpanOrQuery((0..<maxClauses).collect { genSpan(depth + 1) } as SpanQuery[])
        }

        def spanOrGenerator = {
            int depth ->
                new SpanOrQuery((0..<maxClauses).collect { genSpan(depth + 1) } as SpanQuery[])
        }

        def spanNotGenerator = {
            int depth ->
                new SpanNotQuery(genSpan(depth + 1), genSpan(depth + 1))
        }

        def spanTermGenerator = { new SpanTermQuery(randomTerm()) }

        def spanWildcardGenerator = {
            new SpanMultiTermQueryWrapper<>(new WildcardQuery(randomWildcard())).with {
                return it
            }
        }


        def leafGenerators = [
                termGenerator,
                phraseGenerator,
                wildcardGenerator,
                wildcardPhraseGenerator
        ]

        def treeGenerators = [
                booleanQueryGenerator,
                spanNearGenerator
        ]

        def generators = treeGenerators + leafGenerators as List

        def spanTreeGenerators = [
                spanNearGenerator,
                spanOrGenerator,
                spanNotGenerator
        ]

        def spanLeafGenerators = [
                spanTermGenerator,
                spanWildcardGenerator

        ]

        Query genTopQuery() {
            randomElement(r, generators).call(maxDepth)
        }

        Query genQuery(int depth) {
            if (depth < maxDepth) {
                randomElement(r, generators).call(depth)
            } else {
                randomElement(r, leafGenerators).call(depth)
            }
        }

        SpanQuery genSpan(int depth) {
            if (depth < maxDepth) {
                randomElement(r, spanTreeGenerators).call(depth)
            } else {
                randomElement(r, spanLeafGenerators).call(depth)
            }
        }

        Term randomWildcard() {
            new Term(nameField, "${randomPrefix()}*")
        }

        String randomPrefix() {
            def name = dataGen.randomName()
            name.substring(0, r.nextInt(name.length()))
        }

        Term randomTerm() {
            new Term(nameField, dataGen.randomName());
        }

    }

    private class QueriesGenerator implements Iterable<Query> {

        QueryGenerator gen
        int numQueries;

        public QueriesGenerator(QueryGenerator gen, int numQueries) {
            this.numQueries = numQueries
            this.gen = gen
        }

        @Override
        Iterator<Query> iterator() {
            return new Iterator() {
                int count = 0;

                @Override
                boolean hasNext() {
                    return numQueries > count
                }

                @Override
                Query next() {
                    count++
                    return gen.genTopQuery()
                }

                @Override
                void remove() {
                    throw new UnsupportedOperationException("Can't remove from lazy")
                }
            }
        }

    }

    class DataGenerator {

        Random r

        public DataGenerator(Random r) {
            this.r = r
        }

        def randomName() {
            randomElement(r, names)
        }

        Iterable<? extends IndexableField> genDoc() {
            int numWords = r.nextInt(5) + 2
            def value = Joiner.on(' ').join((0..<numWords).collect { randomName() })
            [new TextField(nameField, value, Field.Store.YES)]
        }

        def names = [
                'Adrain',
                'Agustus',
                'Alia',
                'Almond',
                'Alvia',
                'Anwar',
                'Arvo',
                'Ashton',
                'Augustine',
                'Author',
                'Banks',
                'Barbra',
                'Bernardo',
                'Ceil',
                'Channie',
                'Charisse',
                'Clide',
                'Coby',
                'Cristi',
                'Dakoda',
                'Daniele',
                'Daren',
                'Darvin',
                'Davonta',
                'Debbi',
                'Deirdre',
                'Demond',
                'Dock',
                'Dolores',
                'Elgin',
                'Epifanio',
                'Eulalie',
                'Fremont',
                'Hakim',
                'Hans',
                'Hester',
                'Hildegarde',
                'Jackie',
                'Jett',
                'Joann',
                'Judge',
                'Julian',
                'Katlyn',
                'Kayleigh',
                'Keith',
                'Kelly',
                'Kendrick',
                'Kerry',
                'Kyleigh',
                'Lane',
                'Latonya',
                'Latricia',
                'Laureen',
                'Leafy',
                'Lisbeth',
                'Maggie',
                'Marcellus',
                'Marge',
                'Marlee',
                'Marlena',
                'Mayo',
                'Merry',
                'Michial',
                'Mitchel',
                'Mollie',
                'Nathalia',
                'Nathaniel',
                'Neveah',
                'Niles',
                'Norita',
                'Nyree',
                'Opal',
                'Orren',
                'Pamala',
                'Pernell',
                'Prince',
                'Priscilla',
                'Randal',
                'Rebeca',
                'Reinhold',
                'Renata',
                'Reynold',
                'Ronnie',
                'Sabastian',
                'Saint',
                'Samatha',
                'Sarrah',
                'Shakira',
                'Shanelle',
                'Sing',
                'Spenser',
                'Stanford',
                'Stewart',
                'Trae',
                'Tyra',
                'Tyreke',
                'Vic',
                'Wilbur',
                'Will',
                'Willam'

        ]
    }

    class DocIdCollector extends SimpleCollector {

        LeafReaderContext context
        Set<Integer> res = new HashSet<>()

        @Override
        void setScorer(Scorer scorer) throws IOException {

        }

        @Override
        void doSetNextReader(LeafReaderContext context){
            this.context = context
        }

        @Override
        void collect(int doc) throws IOException {
            res.add(context.docBase + doc)
        }

        @Override
        boolean needsScores() {
            return false
        }
    }

}
