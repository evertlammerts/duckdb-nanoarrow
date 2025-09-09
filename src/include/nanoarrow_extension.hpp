//===----------------------------------------------------------------------===//
//                         DuckDB - nanoarrow
//
// nanoarrow_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

class NanoarrowExtension : public Extension {
 public:
  void Load(ExtensionLoader &db) override;
  std::string Name() override;
  std::string Version() const override;
};

}  // namespace duckdb
