//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// table_function/read_arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {
namespace ext_nanoarrow {

TableFunction ReadArrowStreamFunction();

void RegisterReadArrowStream(ExtensionLoader &loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
