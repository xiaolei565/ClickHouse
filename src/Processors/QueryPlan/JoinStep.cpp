#include <Processors/QueryPlan/JoinStep.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/FullSortingMergeJoin.h>

#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_)
    : join(std::move(join_)), max_block_size(max_block_size_), max_streams(max_streams_), keep_left_read_in_order(keep_left_read_in_order_)
{
    input_streams = {left_stream_, right_stream_};
    output_stream = DataStream
    {
        .header = JoiningTransform::transformHeader(left_stream_.header, join),
    };
}

static SortDescription getSortDescription(const Names & key_names)
{
    SortDescription sort_description;
    sort_description.reserve(key_names.size());
    NameSet used_keys;
    for (const auto & key_name : key_names)
    {
        if (!used_keys.insert(key_name).second)
            /// Skip duplicate keys
            continue;

        sort_description.emplace_back(key_name);
    }
    return sort_description;
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & build_settings)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    if (const auto * sorting_join = getSortingJoin())
    {
        chassert(join->pipelineType() == JoinPipelineType::YShaped);

        auto add_sorting = [&build_settings, &sorting_join] (
            std::unique_ptr<SortingStep> & sorting_step,
            QueryPipelineBuilder & pipeline,
            const DataStream & input_stream,
            JoinTableSide join_pos)
        {
            SortDescription sort_description = getSortDescription(sorting_join->getKeyNames(join_pos));

            sorting_step = std::make_unique<SortingStep>(
                input_stream,
                sort_description,
                /* limit */ 0,
                /* settings */ sorting_join->getSortSettings(),
                /* optimize_sorting_by_input_stream_properties */ false);

            const SortDescription & prefix_sort_description = sorting_join->getPrefixSortDesctiption(join_pos);
            if (!prefix_sort_description.empty())
            {
                LOG_DEBUG(&Poco::Logger::get("JoinStep"), "Finish sort {} side of JOIN by [{}] with prefix [{}]",
                    join_pos, dumpSortDescription(sort_description), dumpSortDescription(prefix_sort_description));

                sorting_step->convertToFinishSorting(prefix_sort_description);
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("JoinStep"), "Sort {} side of JOIN by [{}]",
                    join_pos, dumpSortDescription(sort_description), dumpSortDescription(prefix_sort_description));
            }

            sorting_step->transformPipeline(pipeline, build_settings);
        };

        add_sorting(left_sorting, *pipelines[0], input_streams[0], JoinTableSide::Left);
        add_sorting(right_sorting, *pipelines[1], input_streams[1], JoinTableSide::Right);

        auto joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
            std::move(pipelines[0]), std::move(pipelines[1]), join, output_stream->header, max_block_size, &processors);
        joined_pipeline->resize(max_streams);
        return joined_pipeline;
    }
    else if (join->pipelineType() == JoinPipelineType::YShaped)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected join pipeline type for non sorting join");
    }

    return QueryPipelineBuilder::joinPipelinesRightLeft(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        output_stream->header,
        max_block_size,
        max_streams,
        keep_left_read_in_order,
        &processors);
}

bool JoinStep::allowPushDownToRight() const
{
    return join->pipelineType() == JoinPipelineType::YShaped;
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStep::updateInputStream(const DataStream & new_input_stream_, size_t idx)
{
    if (idx == 0)
    {
        input_streams = {new_input_stream_, input_streams.at(1)};
        output_stream = DataStream
        {
            .header = JoiningTransform::transformHeader(new_input_stream_.header, join),
        };
    }
    else
    {
        input_streams = {input_streams.at(0), new_input_stream_};
    }
}

FullSortingMergeJoin * JoinStep::getSortingJoin()
{
    return dynamic_cast<FullSortingMergeJoin *>(join.get());
}

static ITransformingStep::Traits getStorageJoinTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = false,
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilledJoinStep::FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_stream_,
        JoiningTransform::transformHeader(input_stream_.header, join_),
        getStorageJoinTraits())
    , join(std::move(join_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FilledJoinStep expects Join to be filled");
}

void FilledJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    bool default_totals = false;
    if (!pipeline.hasTotals() && join->getTotals())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(header, output_stream->header, join, max_block_size, on_totals, default_totals, counter);
    });
}

void FilledJoinStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), JoiningTransform::transformHeader(input_streams.front().header, join), getDataStreamTraits());
}

}
