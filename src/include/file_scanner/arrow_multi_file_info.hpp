//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// file_scanner/arrow_multi_file_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_function.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! We might have arrow specific options one day
class ArrowFileReaderOptions : public BaseFileReaderOptions {};

struct ArrowMultiFileInfo {
  static unique_ptr<BaseFileReaderOptions> InitializeOptions(
      ClientContext& context, optional_ptr<TableFunctionInfo> info);

  static bool ParseCopyOption(ClientContext& context, const string& key,
                              const vector<Value>& values, BaseFileReaderOptions& options,
                              vector<string>& expected_names,
                              vector<LogicalType>& expected_types);

  static bool ParseOption(ClientContext& context, const string& key, const Value& val,
                          MultiFileOptions& file_options, BaseFileReaderOptions& options);

  static void FinalizeCopyBind(ClientContext& context, BaseFileReaderOptions& options,
                               const vector<string>& expected_names,
                               const vector<LogicalType>& expected_types);

  static unique_ptr<TableFunctionData> InitializeBindData(
      MultiFileBindData& multi_file_data, unique_ptr<BaseFileReaderOptions> options);

  //! This is where the actual binding must happen, so in this function we either:
  //! 1. union_by_name = False. We set the schema/name depending on the first file
  //! 2. union_by_name = True.
  static void BindReader(ClientContext& context, vector<LogicalType>& return_types,
                         vector<string>& names, MultiFileBindData& bind_data);

  static void FinalizeBindData(MultiFileBindData& multi_file_data);

  static void GetBindInfo(const TableFunctionData& bind_data, BindInfo& info);

  static optional_idx MaxThreads(const MultiFileBindData& bind_data_p,
                                 const MultiFileGlobalState& global_state,
                                 FileExpandResult expand_result);

  static unique_ptr<GlobalTableFunctionState> InitializeGlobalState(
      ClientContext& context, MultiFileBindData& bind_data,
      MultiFileGlobalState& global_state);

  static unique_ptr<LocalTableFunctionState> InitializeLocalState(
      ExecutionContext& context, GlobalTableFunctionState& function_state);

  static shared_ptr<BaseFileReader> CreateReader(ClientContext& context,
                                                 GlobalTableFunctionState& gstate,
                                                 BaseUnionData& union_data,
                                                 const MultiFileBindData& bind_data_p);

  static shared_ptr<BaseFileReader> CreateReader(ClientContext& context,
                                                 GlobalTableFunctionState& gstate,
                                                 const OpenFileInfo& file_info,
                                                 idx_t file_idx,
                                                 const MultiFileBindData& bind_data);

  static shared_ptr<BaseFileReader> CreateReader(ClientContext& context,
                                                 const OpenFileInfo& file_info,
                                                 ArrowFileReaderOptions& options,
                                                 const MultiFileOptions& file_options);

  static shared_ptr<BaseUnionData> GetUnionData(shared_ptr<BaseFileReader> scan_p,
                                                idx_t file_idx);

  static void FinalizeReader(ClientContext& context, BaseFileReader& reader,
                             GlobalTableFunctionState&);

  static bool TryInitializeScan(ClientContext& context,
                                const shared_ptr<BaseFileReader>& reader,
                                GlobalTableFunctionState& gstate,
                                LocalTableFunctionState& lstate);

  static void Scan(ClientContext& context, BaseFileReader& reader,
                   GlobalTableFunctionState& global_state,
                   LocalTableFunctionState& local_state, DataChunk& chunk);

  static void FinishFile(ClientContext& context, GlobalTableFunctionState& global_state,
                         BaseFileReader& reader);

  static void FinishReading(ClientContext& context,
                            GlobalTableFunctionState& global_state,
                            LocalTableFunctionState& local_state);

  static unique_ptr<NodeStatistics> GetCardinality(const MultiFileBindData& bind_data,
                                                   idx_t file_count);

  static unique_ptr<BaseStatistics> GetStatistics(ClientContext& context,
                                                  BaseFileReader& reader,
                                                  const string& name);

  static double GetProgressInFile(ClientContext& context, const BaseFileReader& reader);

  static void GetVirtualColumns(ClientContext& context, MultiFileBindData& bind_data,
                                virtual_column_map_t& result);
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
