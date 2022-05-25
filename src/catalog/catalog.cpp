#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    auto offset = sizeof(uint32_t);
    MACH_WRITE_UINT32(buf + offset, table_meta_pages_.size());
    offset += sizeof(uint32_t);
    for (auto &page : table_meta_pages_) {
        MACH_WRITE_UINT32(buf + offset, page.first);
        offset += sizeof(uint32_t);
        MACH_WRITE_INT32(buf + offset, page.second);
        offset += sizeof(int32_t);
    }
    MACH_WRITE_UINT32(buf + offset, index_meta_pages_.size());
    offset += sizeof(uint32_t);
    for (auto &page : index_meta_pages_) {
        MACH_WRITE_UINT32(buf + offset, page.first);
        offset += sizeof(uint32_t);
        MACH_WRITE_INT32(buf + offset, page.second);
        offset += sizeof(int32_t);
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  ASSERT(MACH_READ_UINT32(buf) == CATALOG_METADATA_MAGIC_NUM, "invalid catalog metadata");
  auto offset = sizeof(uint32_t);
  // auto buff = heap->Allocate(sizeof(IndexMetaData));  令人窒息的内存泄漏，警钟长鸣！！！！！
  auto buff = heap->Allocate(sizeof(CatalogMeta));
  auto meta = new (buff) CatalogMeta();
  uint32_t num_table_meta_pages = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  for (uint32_t i = 0; i < num_table_meta_pages; i++) {
    uint32_t table_id = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    int32_t page_id = MACH_READ_INT32(buf + offset);
    offset += sizeof(int32_t);
    meta->table_meta_pages_.insert(std::make_pair(table_id, page_id));
  }
  uint32_t num_index_meta_pages = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  for (uint32_t i = 0; i < num_index_meta_pages; i++) {
    uint32_t index_id = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    int32_t page_id = MACH_READ_INT32(buf + offset);
    offset += sizeof(int32_t);
    meta->index_meta_pages_.insert(std::make_pair(index_id, page_id));
  }
  return meta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(uint32_t) * 3 + sizeof(uint32_t) * 2 * (table_meta_pages_.size() + index_meta_pages_.size());
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
  }
  else {
    catalog_meta_ = CatalogMeta::DeserializeFrom(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData(), heap_);
    auto table_pages = catalog_meta_->GetTableMetaPages();
    for (auto &page : *table_pages) {
      auto table_page = buffer_pool_manager_->FetchPage(page.second);
      table_page->RLatch();
      auto heap = new SimpleMemHeap();
      TableMetadata* table_meta = nullptr;
      TableMetadata::DeserializeFrom(table_page->GetData(), table_meta, heap);
      auto table_heap = TableHeap::Create(buffer_pool_manager_, (int)table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_, heap);
      auto table_info = TableInfo::Create(heap);
      table_info->Init(table_meta, table_heap);
      table_names_.insert(std::make_pair(table_meta->GetTableName(), page.first));
      tables_.insert(std::make_pair(page.first, table_info));
      table_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page.second, false);
    }
    auto index_pages = catalog_meta_->GetIndexMetaPages();
    for (auto &page : *index_pages) {
      auto index_page = buffer_pool_manager_->FetchPage(page.second);
      index_page->RLatch();
      auto heap = new SimpleMemHeap();
      IndexMetadata* index_meta = nullptr;
      IndexMetadata::DeserializeFrom(index_page->GetData(), index_meta, heap);
      auto index_info = IndexInfo::Create(heap);
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      auto table_name = tables_[index_meta->GetTableId()]->GetTableName();
      index_names_[table_name].insert(std::make_pair(index_meta->GetIndexName(), page.first));
      indexes_.insert(std::make_pair(page.first, index_info));
      index_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page.second, false);
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(page_id);
  auto heap = new SimpleMemHeap();
  auto table_heap = TableHeap::Create(buffer_pool_manager_, schema, nullptr, log_manager_, lock_manager_, heap);
  auto table_meta = TableMetadata::Create(next_table_id_, table_name, table_heap->GetFirstPageId(), schema, heap);
  table_info = TableInfo::Create(heap);
  table_info->Init(table_meta, table_heap);
  table_names_.insert(std::make_pair(table_name, table_meta->GetTableId()));
  tables_.insert(std::make_pair(table_meta->GetTableId(), table_info));
  catalog_meta_->GetTableMetaPages()->insert(std::make_pair(table_meta->GetTableId(), page_id));
  page->WLatch();
  table_meta->SerializeTo(page->GetData());
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_names_[table_name]];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto &table : tables_) {
    tables.emplace_back(table.second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  auto table_info = tables_[table_names_[table_name]];
  auto heap = new SimpleMemHeap();
  vector<uint32_t> key_map;
  // bruh
  auto columns = table_info->GetSchema()->GetColumns();
  size_t size = key_map.size();
  for (auto &key : index_keys) {
    for (uint32_t i = 0; i < columns.size(); i++) {
      if (columns[i]->GetName() == key) {
        key_map.emplace_back(i);
      }
    }
    if (size == key_map.size()) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    size++;
  }
  auto index_meta = IndexMetadata::Create(next_index_id_, index_name, table_info->GetTableId(), key_map, heap);
  index_info = IndexInfo::Create(heap);
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  index_names_[table_name].insert(std::make_pair(index_name, index_meta->GetIndexId()));
  indexes_.insert(std::make_pair(index_meta->GetIndexId(), index_info));
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->GetIndexMetaPages()->insert(std::make_pair(index_meta->GetIndexId(), page_id));
  page->WLatch();
  index_meta->SerializeTo(page->GetData());
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, true);
  for (auto iter = table_info->GetTableHeap()->Begin(nullptr); iter != table_info->GetTableHeap()->End(); ++iter) {
    index_info->GetIndex()->InsertEntry(*iter, (*iter).GetRowId(), nullptr);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (index_names_.find(table_name) == index_names_.end()
      || index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index_names_.at(table_name).at(index_name);
  index_info = indexes_.at(index_id);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  for (auto &index_id : index_names_.at(table_name)) {
    indexes.emplace_back(indexes_.at(index_id.second));
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  auto table_info = tables_[table_names_[table_name]];
  delete table_info;
  tables_.erase(table_names_[table_name]);
  catalog_meta_->GetTableMetaPages()->erase(table_names_[table_name]);
  table_names_.erase(table_name);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (index_names_.find(table_name) == index_names_.end()
      || index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index_names_.at(table_name).at(index_name);
  auto index_info = indexes_[index_id];
  delete index_info;
  indexes_.erase(index_id);
  catalog_meta_->GetIndexMetaPages()->erase(index_id);
  index_names_.at(table_name).erase(index_name);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (!page)
    return DB_FAILED;
  page->WLatch();
  catalog_meta_->SerializeTo(page->GetData());
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  return DB_FAILED;
}