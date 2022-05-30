#include "executor/execute_engine.h"
#include "glog/logging.h"

ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  ifstream in("databases.txt");
  if (!in.is_open()) {
    printf("Failed to open databases.txt.\n");
    return DB_FAILED;
  }
  string line;
  while (getline(in, line)) {
    if (line.back() == '\r') {
      line.pop_back();
    }
    if (dbs_.find(line) == dbs_.end())
      dbs_[line] = new DBStorageEngine(line);
  }
  in.close();
  if (dbs_.find(db_name) != dbs_.end()) {
    printf("Database %s already exists.\n", db_name.c_str());
    return DB_FAILED;
  }
  clock_t start = clock();
  dbs_[db_name] = new DBStorageEngine(db_name);
  ofstream out("databases.txt", ios::app);
  out << db_name << endl;
  clock_t end = clock();
  printf("Database %s created in %lf s.\n", db_name.c_str(), (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  ifstream in("databases.txt");
  if (!in.is_open()) {
    printf("Failed to open databases.txt.\n");
    return DB_FAILED;
  }
  string line;
  while (getline(in, line)) {
    if (line.back() == '\r') {
      line.pop_back();
    }
    if (dbs_.find(line) == dbs_.end())
      dbs_[line] = new DBStorageEngine(line);
  }
  in.close();
  if (dbs_.find(db_name) == dbs_.end()) {
    printf("Database %s does not exist.\n", db_name.c_str());
    return DB_FAILED;
  }
  clock_t start = clock();
  delete dbs_[db_name];
  dbs_.erase(db_name);
  ofstream out("databases.txt");
  for (auto & db : dbs_) {
    out << db.first << endl;
  }
  out.close();
  if (current_db_ == db_name) {
    current_db_ = "";
  }
  clock_t end = clock();
  printf("Database %s dropped in %lf s.\n", db_name.c_str(), (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  ifstream in("databases.txt");
  if (!in.is_open()) {
    printf("Failed to open databases.txt.\n");
    return DB_FAILED;
  }
  string line;
  while (getline(in, line)) {
    if (line.back() == '\r') {
      line.pop_back();
    }
    if (dbs_.find(line) == dbs_.end())
      dbs_[line] = new DBStorageEngine(line);
  }
  in.close();
  printf("┌─────────────┐\n");
  printf("│ Database(s) │\n");
  clock_t start = clock();
  for (auto & db : dbs_) {
    printf("├─────────────┤\n");
    printf("│%-13s│\n", db.first.c_str());
  }
  clock_t end = clock();
  printf("└─────────────┘\n");
  printf("%d row(s) returned in %lf s.\n", static_cast<int>(dbs_.size()), (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  ifstream db_file(db_name);
  if (dbs_.find(db_name) == dbs_.end() && !db_file.is_open()) {
    printf("Database %s does not exist.\n", db_name.c_str());
    return DB_FAILED;
  } else if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    return DB_SUCCESS;
  } else {
    dbs_[db_name] = new DBStorageEngine(db_name, false);
    current_db_ = db_name;
    return DB_SUCCESS;
  }
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  vector<TableInfo*> tables;
  printf("┌───────────────────┐\n");
  printf("│ Tables_in_%-7s │\n", current_db_.substr(0, 7).c_str());
  clock_t start = clock();
  db->catalog_mgr_->GetTables(tables);
  for (auto & table : tables) {
    printf("├───────────────────┤\n");
    printf("│%-19s│\n", table->GetTableName().c_str());
  }
  clock_t end = clock();
  printf("└───────────────────┘\n");
  printf("%d row(s) returned in %lf s.\n", static_cast<int>(tables.size()), (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  string table_name = ast->child_->val_;
  TableInfo* table_info = nullptr;
  if (db->catalog_mgr_->GetTable(table_name, table_info) == DB_SUCCESS) {
    printf("Table %s already exists.\n", table_name.c_str());
    return DB_TABLE_ALREADY_EXIST;
  }

  auto column_list = ast->child_->next_->child_;
  auto primary_list = column_list;
  vector<Column*> columns;
  vector<string> primary_keys;
  clock_t start = clock();
  // get primary keys
  while (primary_list) {
    if (primary_list->val_ && strcmp(primary_list->val_, "primary keys") == 0) {
      auto pk = primary_list->child_;
      while (pk) {
        primary_keys.emplace_back(pk->val_);
        pk = pk->next_;
      }
      break;
    }
    primary_list = primary_list->next_;
  }
  // get columns
  uint32_t index = 0;
  while (column_list) {
    if (column_list->val_ && strcmp(column_list->val_, "primary keys") == 0) {
      column_list = column_list->next_;
      continue;
    }
    string name(column_list->child_->val_);
    string type(column_list->child_->next_->val_);
    float length = 0;
    if (type == "char") {
      length = stof(column_list->child_->next_->child_->val_);
      if (length <= -numeric_limits<float>::epsilon()  // negative
          || fabs(length-(int)length) > numeric_limits<float>::epsilon()) {  // not int
        printf("Invalid char length.\n");
        return DB_FAILED;
      }
    }
    bool is_primary = (find(primary_keys.begin(), primary_keys.end(), name) != primary_keys.end());
    bool is_unique = is_primary ? is_primary : (column_list->val_ && strcmp(column_list->val_, "unique") == 0);
    if (type == "char") {
      columns.emplace_back(new Column(name, kTypeChar,
                                      static_cast<uint32_t>(length), index++,
                                      !is_primary, is_unique));
    } else if (type == "int") {
      columns.emplace_back(new Column(name, kTypeInt,
                                      index++, !is_primary, is_unique));
    } else {
      columns.emplace_back(new Column(name, kTypeFloat,
                                      index++, !is_primary, is_unique));
    }
    column_list = column_list->next_;
  }
  // create table
  db->catalog_mgr_->CreateTable(table_name, new Schema(columns),
                                nullptr, table_info);
  for (auto & column : columns) {
    if (column->IsUnique()) {
      IndexInfo* index_info = nullptr;
      vector<string> key(1, column->GetName());
      db->catalog_mgr_->CreateIndex(table_name, column->GetName(),
                                    key, nullptr, index_info);
    }
  }
  clock_t end = clock();
  printf("Table %s created in %lf s.\n", table_name.c_str(), (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  string table_name = ast->child_->val_;
  TableInfo* table_info = nullptr;
  if (db->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS) {
    printf("Table %s does not exist.\n", table_name.c_str());
    return DB_TABLE_NOT_EXIST;
  }
  clock_t start = clock();
  db->catalog_mgr_->DropTable(table_name);
  clock_t end = clock();
  printf("Table %s dropped in %lf s.\n", table_name.c_str(), (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  std::vector<TableInfo *> tables;
  db->catalog_mgr_->GetTables(tables);
  size_t index_num = 0;
  printf("┌────────────────────┐\n");
  printf("│ Indexes_in_%-7s │\n", current_db_.substr(0, 7).c_str());
  clock_t start = clock();
  for (auto& table : tables) {
    vector<IndexInfo *> indexes;
    db->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
    index_num += indexes.size();
    if (!indexes.empty()) {
      printf("├────────────────────┤\n");
      printf("│Table: %-13s│\n", table->GetTableName().c_str());
    }
    for (auto& index : indexes) {
      printf("├────────────────────┤\n");
      printf("│  Index: %-11s│\n", index->GetIndexName().c_str());
      auto columns = index->GetIndexKeySchema()->GetColumns();
      printf("├────────────────────┤\n");
      for (auto& column : columns) {
        printf("│    Column: %-8s│\n", column->GetName().c_str());
      }
    }
  }
  clock_t end = clock();
  printf("└────────────────────┘\n");
  printf("%zu index(es) returned in %lf s.\n", index_num, (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  IndexInfo* index_info = nullptr;
  if (db->catalog_mgr_->GetIndex(table_name, index_name, index_info) == DB_SUCCESS) {
    printf("Index %s already exists.\n", index_name.c_str());
    return DB_INDEX_ALREADY_EXIST;
  }
  clock_t start = clock();
  if (ast->child_->next_->next_->next_) {
    auto key_type = ast->child_->next_->next_->next_;
    if (strcmp(key_type->child_->val_, "btree") != 0) {
      printf("Index type %s not supported.\n", key_type->child_->val_);
      return DB_FAILED;
    }
  }
  auto keys_ = ast->child_->next_->next_->child_;
  vector<string> keys;
  while (keys_) {
    keys.emplace_back(keys_->val_);
    keys_ = keys_->next_;
  }
  auto dberr = db->catalog_mgr_->CreateIndex(table_name, index_name, keys, nullptr, index_info);
  if (dberr == DB_FAILED) {
    printf("Cannot create index %s on un-unique key.\n", index_name.c_str());
    return DB_FAILED;
  }
  clock_t end = clock();
  printf("Index %s created in %lf s.\n", index_name.c_str(), (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  string index_name = ast->child_->val_;
  int count = 0;
  vector<TableInfo *> tables;
  clock_t start = clock();
  db->catalog_mgr_->GetTables(tables);
  for (auto& table : tables) {
    IndexInfo * index_info;
    if (db->catalog_mgr_->GetIndex(table->GetTableName(),
                                   index_name, index_info) != DB_INDEX_NOT_FOUND) {
      count++;
      db->catalog_mgr_->DropIndex(table->GetTableName(), index_name);
      printf("Index %s on %s dropped.\n", index_name.c_str(), table->GetTableName().c_str());
    }
  }
  clock_t end = clock();
  if (!count) {
    printf("Index %s not found.\n", index_name.c_str());
    return DB_INDEX_NOT_FOUND;
  }
  printf("%d index(es) dropped in %lf s.\n", count, (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

using INDEX_KEY_TYPE = GenericKey<32>;
using INDEX_COMPARATOR_TYPE = GenericComparator<32>;
using BP_TREE_INDEX = BPlusTreeIndex<INDEX_KEY_TYPE, RowId, INDEX_COMPARATOR_TYPE>;

dberr_t ExecuteQuery(pSyntaxNode& condition, vector<RowId>& range, TableInfo* info, vector<IndexInfo*> indexes) {
  IndexInfo* idx = nullptr;
  for (auto& index : indexes) {
    if (index->GetIndexKeySchema()->GetColumn(0)->GetName() == condition->child_->val_ &&
        index->GetIndexKeySchema()->GetColumns().size() == 1) {
      idx = index;
      break;
    }
  }
  uint32_t column_idx;
  if (info->GetSchema()->GetColumnIndex(condition->child_->val_, column_idx) == DB_COLUMN_NAME_NOT_EXIST)
    return DB_FAILED;
  auto column = info->GetSchema()->GetColumn(column_idx);
  vector<Field> key_value;

  if (condition->child_->next_->type_ != kNodeNull) {
    char* value = (char*)malloc(column->GetLength()+1);
    memset(value, 0, column->GetLength()+1);
    strcpy(value, condition->child_->next_->val_);
    if (column->GetType() == kTypeInt)
      key_value.emplace_back(Field(kTypeInt, stoi(value)));
    else if (column->GetType() == kTypeFloat)
      key_value.emplace_back(Field(kTypeFloat, stof(value)));
    else if (column->GetType() == kTypeChar)
      key_value.emplace_back(Field(kTypeChar, value, column->GetLength(), true));
  } else {
    key_value.emplace_back(Field(column->GetType()));
  }

  if (range.empty() && idx && strcmp(condition->val_, "<>") != 0) {
    auto btreeidx = reinterpret_cast<BP_TREE_INDEX*>(idx->GetIndex());
    INDEX_KEY_TYPE key;
    key.SerializeFromKey(Row(key_value), idx->GetIndexKeySchema());
    if (strcmp(condition->val_, "=") == 0) {
      btreeidx->ScanKey(Row(key_value), range, nullptr);
    } else if (strcmp(condition->val_, ">") == 0) {
      bool it = true;
      for (auto iter = btreeidx->GetBeginIterator(key); iter != btreeidx->GetEndIterator(); ++iter) {
        if (it) {
          it = false;
          continue;
        }
        range.emplace_back((*iter).second);
      }
    } else if (strcmp(condition->val_, "<") == 0) {
      for (auto iter = btreeidx->GetBeginIterator(); iter != btreeidx->GetBeginIterator(key); ++iter) {
        range.emplace_back((*iter).second);
      }
    } else if (strcmp(condition->val_, ">=") == 0) {
      for (auto iter = btreeidx->GetBeginIterator(key); iter != btreeidx->GetEndIterator(); ++iter) {
        range.emplace_back((*iter).second);
      }
    } else if (strcmp(condition->val_, "<=") == 0) {
      for (auto iter = btreeidx->GetBeginIterator(); iter != btreeidx->GetBeginIterator(key); ++iter) {
        range.emplace_back((*iter).second);
      }
      range.emplace_back((*btreeidx->GetBeginIterator(key)).second);
    } else {
      return DB_FAILED;
    }

  } else if (range.empty()){
    if (strcmp(condition->val_, "=") == 0) {
      for (auto iter = info->GetTableHeap()->Begin(nullptr); iter!= info->GetTableHeap()->End(); ++iter) {
        if ((*iter).GetField(column_idx)->CompareEquals(key_value[0]) == CmpBool::kTrue) {
          range.emplace_back(iter->GetRowId());
        }
      }
    } else if (strcmp(condition->val_, ">") == 0) {
      for (auto iter = info->GetTableHeap()->Begin(nullptr); iter!= info->GetTableHeap()->End(); ++iter) {
        if ((*iter).GetField(column_idx)->CompareGreaterThan(key_value[0]) == CmpBool::kTrue) {
          range.emplace_back(iter->GetRowId());
        }
      }
    } else if (strcmp(condition->val_, "<") == 0) {
      for (auto iter = info->GetTableHeap()->Begin(nullptr); iter != info->GetTableHeap()->End(); ++iter) {
        if ((*iter).GetField(column_idx)->CompareLessThan(key_value[0]) == CmpBool::kTrue) {
          range.emplace_back(iter->GetRowId());
        }
      }
    } else if (strcmp(condition->val_, ">=") == 0) {
      for (auto iter = info->GetTableHeap()->Begin(nullptr); iter!= info->GetTableHeap()->End(); ++iter) {
        if ((*iter).GetField(column_idx)->CompareGreaterThanEquals(key_value[0]) == CmpBool::kTrue) {
          range.emplace_back(iter->GetRowId());
        }
      }
    } else if (strcmp(condition->val_, "<=") == 0) {
      for (auto iter = info->GetTableHeap()->Begin(nullptr); iter != info->GetTableHeap()->End(); ++iter) {
        if ((*iter).GetField(column_idx)->CompareLessThanEquals(key_value[0]) == CmpBool::kTrue) {
          range.emplace_back(iter->GetRowId());
        }
      }
    } else if (strcmp(condition->val_, "<>") == 0) {
      for (auto iter = info->GetTableHeap()->Begin(nullptr); iter!= info->GetTableHeap()->End(); ++iter) {
        if ((*iter).GetField(column_idx)->CompareNotEquals(key_value[0]) == CmpBool::kTrue) {
          range.emplace_back(iter->GetRowId());
        }
      }
    } else if (strcmp(condition->val_, "is") == 0) {
      for (auto iter = info->GetTableHeap()->Begin(nullptr); iter!= info->GetTableHeap()->End(); ++iter) {
        if ((*iter).GetField(column_idx)->IsNull()) {
          range.emplace_back(iter->GetRowId());
        }
      }
    } else if (strcmp(condition->val_, "not") == 0) {
      for (auto iter = info->GetTableHeap()->Begin(nullptr); iter != info->GetTableHeap()->End(); ++iter) {
        if (!(*iter).GetField(column_idx)->IsNull()) {
          range.emplace_back(iter->GetRowId());
        }
      }
    }


  } else {
    vector<RowId> new_range;
    if (strcmp(condition->val_, "=") == 0) {
      for (auto iter: range) {
        Row r(iter);
        info->GetTableHeap()->GetTuple(&r, nullptr);
        if (r.GetField(column_idx)->CompareEquals(key_value[0]) == CmpBool::kTrue) {
          new_range.emplace_back(iter);
        }
      }
    } else if (strcmp(condition->val_, ">") == 0) {
      for (auto iter: range) {
        Row r(iter);
        info->GetTableHeap()->GetTuple(&r, nullptr);
        if (r.GetField(column_idx)->CompareGreaterThan(key_value[0]) == CmpBool::kTrue) {
          new_range.emplace_back(iter);
        }
      }
    } else if (strcmp(condition->val_, "<") == 0) {
      for (auto iter: range) {
        Row r(iter);
        info->GetTableHeap()->GetTuple(&r, nullptr);
        if (r.GetField(column_idx)->CompareLessThan(key_value[0]) == CmpBool::kTrue) {
          new_range.emplace_back(iter);
        }
      }
    } else if (strcmp(condition->val_, ">=") == 0) {
      for (auto iter: range) {
        Row r(iter);
        info->GetTableHeap()->GetTuple(&r, nullptr);
        if (r.GetField(column_idx)->CompareGreaterThanEquals(key_value[0]) == CmpBool::kTrue) {
          new_range.emplace_back(iter);
        }
      }
    } else if (strcmp(condition->val_, "<=") == 0) {
      for (auto iter: range) {
        Row r(iter);
        info->GetTableHeap()->GetTuple(&r, nullptr);
        if (r.GetField(column_idx)->CompareLessThanEquals(key_value[0]) == CmpBool::kTrue) {
          new_range.emplace_back(iter);
        }
      }
    } else if (strcmp(condition->val_, "<>") == 0) {
      for (auto iter: range) {
        Row r(iter);
        info->GetTableHeap()->GetTuple(&r, nullptr);
        if (r.GetField(column_idx)->CompareNotEquals(key_value[0]) == CmpBool::kTrue) {
          new_range.emplace_back(iter);
        }
      }
    } else if (strcmp(condition->val_, "is") == 0) {
      for (auto iter: range) {
        Row r(iter);
        info->GetTableHeap()->GetTuple(&r, nullptr);
        if (r.GetField(column_idx)->IsNull()) {
          new_range.emplace_back(iter);
        }
      }
    } else if (strcmp(condition->val_, "not") == 0) {
      for (auto iter : range) {
        Row r(iter);
        info->GetTableHeap()->GetTuple(&r, nullptr);
        if (!r.GetField(column_idx)->IsNull()) {
          new_range.emplace_back(iter);
        }
      }
    }
    range = new_range;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  vector<string> column_names;
  vector<vector<RowId>> row_ids;
  list<RowId> ans;
  clock_t start = clock();
  if (ast->child_->child_) {
    auto columns = ast->child_->child_;
    while (columns) {
      column_names.emplace_back(columns->val_);
      columns = columns->next_;
    }
  }
  string table_name = ast->child_->next_->val_;
  TableInfo* table_info = nullptr;
  db->catalog_mgr_->GetTable(table_name, table_info);
  if (!table_info) {
    printf("Table %s not found.\n", table_name.c_str());
    return DB_TABLE_NOT_EXIST;
  }

  if (ast->child_->next_->next_) {  // have where clause
    vector<IndexInfo*> indexes;
    db->catalog_mgr_->GetTableIndexes(table_name, indexes);
    list<pSyntaxNode> conditions;
    auto where = ast->child_->next_->next_->child_;
    while (where->child_) {
      conditions.emplace_front(where);
      where = where->child_;
    }
    while (!conditions.empty()) {
      auto condition = conditions.front();
      conditions.pop_front();
      if (strcmp(condition->val_, "and") != 0 && strcmp(condition->val_, "or") != 0) {
        if (row_ids.empty())
          row_ids.emplace_back(vector<RowId>());
        if (ExecuteQuery(condition, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", condition->child_->val_);
          return DB_FAILED;
        }
      } else if (strcmp(condition->val_, "and") == 0) {
        if (row_ids.empty())
          row_ids.emplace_back(vector<RowId>());
        auto right = condition->child_->next_;
        if (ExecuteQuery(right, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", right->child_->val_);
          return DB_FAILED;
        }
      } else if (strcmp(condition->val_, "or") == 0) {
        auto right = condition->child_->next_;
        vector<RowId> new_row_id;
        row_ids.emplace_back(new_row_id);
        if (ExecuteQuery(right, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", right->child_->val_);
          return DB_FAILED;
        }
      }
    }
  }
  for (auto& row_id : row_ids) {
    for (auto& id : row_id) {
      ans.emplace_back(id);
    }
  }
  ans.sort();
  ans.unique();
  int row_count = 0;
  printf("┌");
  if (column_names.empty()) {
    for (uint32_t i = 0; i < table_info->GetSchema()->GetColumnCount() - 1; ++i) {
      printf("──────────┬");
    }
    printf("──────────┐\n");
    printf("│");
    for (auto& c : table_info->GetSchema()->GetColumns()) {
      printf("%-10s│", c->GetName().c_str());
    }
    printf("\n");
    printf("├");
    for (uint32_t i = 0; i < table_info->GetSchema()->GetColumnCount() - 1; ++i) {
      printf("──────────┼");
    }
    printf("──────────┤\n");
  } else {
    for (uint32_t i = 0; i < column_names.size() - 1; ++i) {
      printf("──────────┬");
    }
    printf("──────────┐\n");
    for (auto& c : column_names) {
      printf("%-10s│", c.c_str());
    }
    printf("\n");
    printf("├");
    for (uint32_t i = 0; i < column_names.size() - 1; ++i) {
      printf("──────────┼");
    }
    printf("──────────┤\n");
  }
  if (!ast->child_->next_->next_) {
    for (auto iter = table_info->GetTableHeap()->Begin(nullptr); iter != table_info->GetTableHeap()->End(); ++iter) {
      printf("│");
      row_count++;
      Row r = *iter;
      if (column_names.empty()) {
        for (auto& field : r.GetFields()) {
          if (field->IsNull()) {
            printf("NULL      │");
          } else {
            printf("%-10s│", field->GetData());
          }
        }
        printf("\n");
      } else {
        for (auto& column_name : column_names) {
          uint32_t column_idx;
          table_info->GetSchema()->GetColumnIndex(column_name, column_idx);
          auto field = r.GetField(column_idx);
          if (field->IsNull()) {
            printf("NULL      │");
          } else {
            printf("%-10s│", field->GetData());
          }
        }
        printf("\n");
      }
    }
  } else {
    for (auto& row_id : ans) {
      printf("│");
      row_count++;
      Row r(row_id);
      table_info->GetTableHeap()->GetTuple(&r, nullptr);
      if (column_names.empty()) {
        for (auto& field : r.GetFields()) {
          if (field->IsNull()) {
            printf("NULL      │");
          } else {
            printf("%-10s│", field->GetData());
          }
        }
        printf("\n");
      } else {
        for (auto& column_name : column_names) {
          uint32_t column_idx;
          table_info->GetSchema()->GetColumnIndex(column_name, column_idx);
          auto field = r.GetField(column_idx);
          if (field->IsNull()) {
            printf("NULL      │");
          } else {
            printf("%-10s│", field->GetData());
          }
        }
        printf("\n");
      }
    }
  }
  clock_t end = clock();
  printf("└");
  if (column_names.empty()) {
    for (uint32_t i = 0; i < table_info->GetSchema()->GetColumnCount() - 1; ++i) {
      printf("──────────┴");
    }
    printf("──────────┘\n");
  } else {
    for (uint32_t i = 0; i < column_names.size() - 1; ++i) {
      printf("──────────┴");
    }
    printf("──────────┘\n");
  }
  printf("%d row(s) selected in %lf s.\n", row_count, (double)(end - start) / CLOCKS_PER_SEC);
  return DB_FAILED;
}
#define IDX_TEST
dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  string table_name = ast->child_->val_;
  auto values = ast->child_->next_->child_;
  TableInfo* table_info = nullptr;
  db->catalog_mgr_->GetTable(table_name, table_info);
  if (!table_info) {
    printf("Table %s not found.\n", table_name.c_str());
    return DB_TABLE_NOT_EXIST;
  }
  clock_t start = clock();
  vector<SyntaxNodeType> types;
  vector<string> row_values;
  auto schema = table_info->GetSchema();
  while (values) {
    types.emplace_back(values->type_);
    if (values->val_)
      row_values.emplace_back(values->val_);
    else
      row_values.emplace_back("");
    values = values->next_;
  }
  if (row_values.size() != schema->GetColumnCount()) {
    printf("Invalid number of values.\n");
    return DB_FAILED;
  }
  vector<int> unique_col_idx;
  vector<string> unique_col;
  auto column = schema->GetColumns();
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if ((types[i] == kNodeNumber && column[i]->GetType() == kTypeChar) ||
        (types[i] == kNodeString && column[i]->GetType() != kTypeChar) ||
        (types[i] == kNodeNull && !column[i]->IsNullable())) {
      printf("Invalid type for column %s.\n", column[i]->GetName().c_str());
      return DB_FAILED;
    }
    if (column[i]->IsUnique()) {
      unique_col.emplace_back(column[i]->GetName());
      unique_col_idx.emplace_back(i);
    }
  }
  auto table_heap = table_info->GetTableHeap();

  vector<Field> fields;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    char* value = (char*)malloc(schema->GetColumn(i)->GetLength()+1);
    memset(value, 0, schema->GetColumn(i)->GetLength()+1);
    strcpy(value, row_values[i].c_str());
    if (types[i] == kNodeNull) {
      fields.emplace_back(Field(column[i]->GetType()));
    } else if (column[i]->GetType() == kTypeChar) {
      fields.emplace_back(Field(kTypeChar, value,
                                column[i]->GetLength(),
                                (column[i]->GetLength() != 0)));
    } else if (column[i]->GetType() == kTypeInt) {
      fields.emplace_back(Field(kTypeInt, stoi(value)));
    } else {
      fields.emplace_back(Field(kTypeFloat, stof(value)));
    }
  }
  Row r(fields);
  // index operation
  vector<IndexInfo *> indexes;
  db->catalog_mgr_->GetTableIndexes(table_name, indexes);

  if (!unique_col.empty()) {
#ifndef IDX_TEST
    // simper traverse
    for (auto iter = table_heap->Begin(nullptr); iter != table_heap->End(); iter++) {
      auto row = *iter;
      for (auto& i : unique_col_idx) {
        if (r.GetField(i)->CompareEquals(*row.GetField(i)) == CmpBool::kTrue) {
          printf("Duplicate key for column %s.\n", column[i]->GetName().c_str());
          return DB_FAILED;
        }
      }
    }
#else
    // index traverse
    for (uint32_t i = 0; i < unique_col.size(); i++) {
      bool flag = false;
      for (auto& index : indexes) {
        if (index->GetIndexKeySchema()->GetColumn(0)->GetName() == unique_col[i] &&
            index->GetIndexKeySchema()->GetColumns().size() == 1) {
          vector<RowId> row_ids;
          vector<Field> row_fields(1, *r.GetField(unique_col_idx[i]));
          index->GetIndex()->ScanKey(Row(row_fields), row_ids, nullptr);
          flag = true;
          if (!row_ids.empty()) {
            printf("Duplicate key for column %s.\n", unique_col[i].c_str());
            return DB_FAILED;
          }
        }
      }
      if (!flag) {
        printf("Index for unique column %s not found.\n", unique_col[i].c_str());
        return DB_FAILED;
      }
    }
#endif
  }
  if (!table_heap->InsertTuple(r, nullptr)) {
    printf("Insert failed.\n");
    return DB_FAILED;
  }
  for (auto& index : indexes) {
    auto key_map = index->GetKeyMapping();
    vector<Field> index_fields;
    for (auto& i : key_map) {
      index_fields.emplace_back(*r.GetField(i));
    }
    index->GetIndex()->InsertEntry(Row(index_fields), r.GetRowId(), nullptr);
  }
  clock_t end = clock();
  printf("1 row inserted in %lf s.\n", (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  vector<vector<RowId>> row_ids;
  list<RowId> ans;
  clock_t start = clock();
  string table_name = ast->child_->val_;
  TableInfo* table_info = nullptr;
  db->catalog_mgr_->GetTable(table_name, table_info);
  if (!table_info) {
    printf("Table %s not found.\n", table_name.c_str());
    return DB_TABLE_NOT_EXIST;
  }
  vector<IndexInfo*> indexes;
  db->catalog_mgr_->GetTableIndexes(table_name, indexes);
  if (ast->child_->next_) {  // have where clause
    list<pSyntaxNode> conditions;
    auto where = ast->child_->next_->child_;
    while (where->child_) {
      conditions.emplace_front(where);
      where = where->child_;
    }
    while (!conditions.empty()) {
      auto condition = conditions.front();
      conditions.pop_front();
      if (strcmp(condition->val_, "and") != 0 && strcmp(condition->val_, "or") != 0) {
        if (row_ids.empty())
          row_ids.emplace_back(vector<RowId>());
        if (ExecuteQuery(condition, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", condition->child_->val_);
          return DB_FAILED;
        }
      } else if (strcmp(condition->val_, "and") == 0) {
        if (row_ids.empty())
          row_ids.emplace_back(vector<RowId>());
        auto right = condition->child_->next_;
        if (ExecuteQuery(right, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", right->child_->val_);
          return DB_FAILED;
        }
      } else if (strcmp(condition->val_, "or") == 0) {
        auto right = condition->child_->next_;
        vector<RowId> new_row_id;
        row_ids.emplace_back(new_row_id);
        if (ExecuteQuery(right, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", right->child_->val_);
          return DB_FAILED;
        }
      }
    }
  }
  for (auto& row_id : row_ids) {
    for (auto& id : row_id) {
      ans.emplace_back(id);
    }
  }
  ans.sort();
  ans.unique();
  int row_count = 0;
  if (!ast->child_->next_) {
    for (auto iter = table_info->GetTableHeap()->Begin(nullptr);
         iter != table_info->GetTableHeap()->End(); ++iter) {
      row_count++;
      auto r = new Row(iter->GetRowId());
      table_info->GetTableHeap()->GetTuple(r, nullptr);
      for (auto& index : indexes) {
        vector<Field> index_fields;
        for (auto& i : index->GetKeyMapping()) {
          index_fields.emplace_back(*r->GetField(i));
        }
        index->GetIndex()->RemoveEntry(Row(index_fields), iter->GetRowId(), nullptr);
      }
      table_info->GetTableHeap()->ApplyDelete(iter->GetRowId(), nullptr);
    }
  } else {
    for (auto& i : ans) {
      row_count++;
      auto r = new Row(i);
      table_info->GetTableHeap()->GetTuple(r, nullptr);
      for (auto& index : indexes) {
        vector<Field> index_fields;
        for (auto& j : index->GetKeyMapping()) {
          index_fields.emplace_back(*r->GetField(j));
        }
        index->GetIndex()->RemoveEntry(Row(index_fields), i, nullptr);
      }
      table_info->GetTableHeap()->ApplyDelete(i, nullptr);
    }
  }
  table_info->GetTableHeap()->RecreateQueue();
  clock_t end = clock();
  printf("%d rows deleted in %lf s.\n", row_count, (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  if (current_db_.empty()) {
    printf("No database selected.\n");
    return DB_FAILED;
  }
  auto db = dbs_[current_db_];
  map<uint32_t, string> value_map;
  vector<vector<RowId>> row_ids;
  list<RowId> ans;
  clock_t start = clock();
  string table_name = ast->child_->val_;
  TableInfo* table_info = nullptr;
  db->catalog_mgr_->GetTable(table_name, table_info);
  if (!table_info) {
    printf("Table %s not found.\n", table_name.c_str());
    return DB_TABLE_NOT_EXIST;
  }

  auto values_table = ast->child_->next_->child_;
  while (values_table) {
    uint32_t index;
    if (table_info->GetSchema()->GetColumnIndex(values_table->child_->val_, index) == DB_FAILED) {
      printf("Not exist column name %s.\n", values_table->child_->val_);
      return DB_FAILED;
    }
    value_map[index] = values_table->child_->next_->val_;
    values_table = values_table->next_;
  }

  if (ast->child_->next_->next_) {  // have where clause
    vector<IndexInfo*> indexes;
    db->catalog_mgr_->GetTableIndexes(table_name, indexes);
    list<pSyntaxNode> conditions;
    auto where = ast->child_->next_->next_->child_;
    while (where->child_) {
      conditions.emplace_front(where);
      where = where->child_;
    }
    while (!conditions.empty()) {
      auto condition = conditions.front();
      conditions.pop_front();
      if (strcmp(condition->val_, "and") != 0 && strcmp(condition->val_, "or") != 0) {
        if (row_ids.empty())
          row_ids.emplace_back(vector<RowId>());
        if (ExecuteQuery(condition, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", condition->child_->val_);
          return DB_FAILED;
        }
      } else if (strcmp(condition->val_, "and") == 0) {
        if (row_ids.empty())
          row_ids.emplace_back(vector<RowId>());
        auto right = condition->child_->next_;
        if (ExecuteQuery(right, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", right->child_->val_);
          return DB_FAILED;
        }
      } else if (strcmp(condition->val_, "or") == 0) {
        auto right = condition->child_->next_;
        vector<RowId> new_row_id;
        row_ids.emplace_back(new_row_id);
        if (ExecuteQuery(right, row_ids.back(), table_info, indexes) == DB_FAILED) {
          printf("Not exist column name %s.\n", right->child_->val_);
          return DB_FAILED;
        }
      }
    }
  }
  for (auto& row_id : row_ids) {
    for (auto& id : row_id) {
      ans.emplace_back(id);
    }
  }
  ans.sort();
  ans.unique();
  int row_count = 0;
  // unique update fail
  if (!ast->child_->next_->next_ || ans.size() > 1) {
    for (auto i : value_map) {
      if (table_info->GetSchema()->GetColumn(i.first)->IsUnique()) {
        printf("Cannot update unique column %s.\n", table_info->GetSchema()->GetColumn(i.first)->GetName().c_str());
        return DB_FAILED;
      }
    }
  }
  vector<IndexInfo*> indexes;
  db->catalog_mgr_->GetTableIndexes(table_name, indexes);
  if (!ast->child_->next_->next_) {
    for (auto iter = table_info->GetTableHeap()->Begin(nullptr); iter != table_info->GetTableHeap()->End(); ++iter) {
      row_count++;
      vector<Field> fields;
      for (uint32_t i = 0; i < table_info->GetSchema()->GetColumnCount(); ++i) {
        if (value_map.find(i) != value_map.end()) {
          if (table_info->GetSchema()->GetColumn(i)->GetType() == kTypeInt) {
            fields.emplace_back(Field(kTypeInt, stoi(value_map[i])));
          } else if (table_info->GetSchema()->GetColumn(i)->GetType() == kTypeFloat) {
            fields.emplace_back(Field(kTypeFloat, stof(value_map[i])));
          } else {
            fields.emplace_back(Field(kTypeChar, (char*)value_map[i].c_str(),
                                        table_info->GetSchema()->GetColumn(i)->GetLength(), true));
          }
        } else {
          fields.emplace_back(*iter->GetField(i));
        }
      }
      auto new_row = new Row(fields);
      new_row->SetRowId(iter->GetRowId());
      table_info->GetTableHeap()->UpdateTuple(*new_row, iter->GetRowId(), nullptr);
    }
  } else {
    for (auto& r_id : ans) {
      row_count++;
      auto iter = new Row(r_id);
      table_info->GetTableHeap()->GetTuple(iter, nullptr);
      if (ans.size() == 1) {
        for (auto &index : indexes) {
          auto key_map = index->GetKeyMapping();
          vector<Field> index_fields;
          for (auto &i : key_map) {
            index_fields.emplace_back(*iter->GetField(i));
          }
          index->GetIndex()->RemoveEntry(Row(index_fields), iter->GetRowId(), nullptr);
        }
      }
      vector<Field> fields;
      for (uint32_t i = 0; i < table_info->GetSchema()->GetColumnCount(); ++i) {
        if (value_map.find(i) != value_map.end()) {
          if (table_info->GetSchema()->GetColumn(i)->GetType() == kTypeInt) {
            fields.emplace_back(Field(kTypeInt, stoi(value_map[i])));
          } else if (table_info->GetSchema()->GetColumn(i)->GetType() == kTypeFloat) {
            fields.emplace_back(Field(kTypeFloat, stof(value_map[i])));
          } else {
              fields.emplace_back(Field(kTypeChar, (char*)value_map[i].c_str(),
                                        table_info->GetSchema()->GetColumn(i)->GetLength(), true));
            }
        } else {
          fields.emplace_back(*iter->GetField(i));
        }
      }
      auto new_row = new Row(fields);
      new_row->SetRowId(iter->GetRowId());
      table_info->GetTableHeap()->UpdateTuple(*new_row, iter->GetRowId(), nullptr);
      if (ans.size() == 1) {
        for (auto &index : indexes) {
          auto key_map = index->GetKeyMapping();
          vector<Field> index_fields;
          for (auto &i : key_map) {
            index_fields.emplace_back(*new_row->GetField(i));
          }
          index->GetIndex()->InsertEntry(Row(index_fields), new_row->GetRowId(), nullptr);
        }
      }
    }
  }

  clock_t end = clock();
  printf("%d rows updated in %lf s.\n", row_count, (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string filename = ast->child_->val_;
  ifstream file(filename);
  if (!file.is_open()) {
    printf("File %s not found.\n", filename.c_str());
    return DB_FAILED;
  }
  string line;
  string cmd;
  int count = 0;
  clock_t start = clock();
  while (getline(file, line)) {
    if (line.empty()) {
      continue;
    }
    cmd += line;
    if (cmd.back() == '\r')
      cmd.pop_back();
    if (cmd.back() == ';') {
      YY_BUFFER_STATE bp = yy_scan_string(cmd.c_str());
      yy_switch_to_buffer(bp);
      MinisqlParserInit();
      yyparse();
      // parse result handle
      if (MinisqlParserGetError())
        // error
        printf("%s\n", MinisqlParserGetErrorMessage());
      ExecuteContext context_02;
      Execute(MinisqlGetParserRootNode(), &context_02);
      // sleep(1);
      // clean memory after parse
      cmd.clear();
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();
      count++;
      if (context_02.flag_quit_) {
        context->flag_quit_ = true;
        break;
      }
    }
  }
  clock_t end = clock();
  file.close();
  printf("%d line(s) executed in %lf s.\n",
         count, (double)(end - start) / CLOCKS_PER_SEC);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  for (auto& i : dbs_) {
    delete i.second;
  }
  return DB_SUCCESS;
}
