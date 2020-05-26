package io.crate.execution.engine.collect;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.Projections;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.InputColumn;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataTypes;

public class SumIntAggregate {


    @Nullable
    public static BatchIterator<Row> tryOptimize(IndexShard indexShard,
                                                 DocTableInfo table,
                                                 LuceneQueryBuilder luceneQueryBuilder,
                                                 FieldTypeLookup fieldTypeLookup,
                                                 DocInputFactory docInputFactory,
                                                 RoutedCollectPhase phase,
                                                 CollectTask collectTask) {
        var shardProjections = Projections.shardProjections(phase.projections());
        Aggregation intSum = sumIntAggregate(shardProjections);
        if (intSum == null) {
            return null;
        }
        var arg = intSum.inputs().get(0);
        if (!(arg instanceof InputColumn)) {
            return null;
        }
        int index = ((InputColumn) arg).index();
        var column = phase.toCollect().get(index);
        if (!(column instanceof Reference)) {
            return null;
        }
        Reference ref = ((Reference) column);
        MappedFieldType fieldType = fieldTypeLookup.get(ref.column().fqn());
        if (fieldType == null || !fieldType.hasDocValues()) {
            return null;
        }
        ShardId shardId = indexShard.shardId();
        SharedShardContext shardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        Searcher searcher = shardContext.acquireSearcher(LuceneShardCollectorProvider.formatSource(phase));
        try {
            QueryShardContext queryShardContext = shardContext.indexService().newQueryShardContext();
            collectTask.addSearcher(shardContext.readerId(), searcher);
            LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
                phase.where(),
                collectTask.txnCtx(),
                indexShard.mapperService(),
                indexShard.shardId().getIndexName(),
                queryShardContext,
                table,
                shardContext.indexService().cache()
            );

            AtomicReference<Throwable> killed = new AtomicReference<>();
            return CollectingBatchIterator.newInstance(
                () -> killed.set(BatchIterator.CLOSED),
                killed::set,
                () -> {
                    try {
                        return CompletableFuture.completedFuture(getRow(
                            searcher,
                            queryContext.query(),
                            ref.column().fqn()
                        ));
                    } catch (Throwable t) {
                        return CompletableFuture.failedFuture(t);
                    }
                },
                true
            );
        } catch (Throwable t) {
            searcher.close();
            throw t;
        }
    }


    private static Iterable<Row> getRow(Searcher searcher,
                                        Query query,
                                        String fqColumnName) throws IOException {
        IndexSearcher indexSearcher = searcher.searcher();
        Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        List<LeafReaderContext> leaves = indexSearcher.getTopReaderContext().leaves();
        long sum = 0L;
        for (var leaf : leaves) {
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            SortedNumericDocValues values = DocValues.getSortedNumeric(leaf.reader(), fqColumnName);
            DocIdSetIterator docs = scorer.iterator();
            Bits liveDocs = leaf.reader().getLiveDocs();
            for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                if (liveDocs != null && !liveDocs.get(doc)) {
                    continue;
                }
                if (values.advanceExact(doc) && values.docValueCount() == 1) {
                    sum += values.nextValue();
                }
            }
        }
        return List.of(new Row1(sum));
    }


    @Nullable
    private static Aggregation sumIntAggregate(Collection<? extends Projection> shardProjections) {
        if (shardProjections.size() != 1) {
            return null;
        }
        var projection = shardProjections.iterator().next();
        if (!(projection instanceof AggregationProjection)) {
            return null;
        }
        AggregationProjection aggProjection = (AggregationProjection) projection;
        if (aggProjection.aggregations().size() != 1) {
            return null;
        }
        var agg = aggProjection.aggregations().get(0);
        if (agg.functionIdent().name().equals("sum") && agg.valueType().equals(DataTypes.LONG)) {
            return agg;
        }
        return null;
    }
}
