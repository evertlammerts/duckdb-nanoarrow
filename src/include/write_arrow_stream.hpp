//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// write_arrow_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace ext_nanoarrow {

void RegisterArrowStreamCopyFunction(ExtensionLoader &loader);

}  // namespace ext_nanoarrow
}  // namespace duckdb
