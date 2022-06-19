#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
  auto offset = sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + offset, table_id_);
  offset += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + offset, table_name_.size());
  offset += sizeof(uint32_t);
  memcpy(buf + offset, table_name_.c_str(), table_name_.size());
  offset += table_name_.size();
  MACH_WRITE_INT32(buf + offset, root_page_id_);
  offset += sizeof(int32_t);
  schema_->SerializeTo(buf + offset);
  offset += schema_->GetSerializedSize();
  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(uint32_t) + sizeof(uint32_t) + table_name_.size() +
         sizeof(int32_t) + schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  ASSERT(MACH_READ_UINT32(buf) == TABLE_METADATA_MAGIC_NUM, "invalid table metadata");
  auto offset = sizeof(uint32_t);
  uint32_t table_id = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  uint32_t table_name_size = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  std::string table_name;
  for (std::string::size_type i = 0; i < table_name_size; i++)
    table_name += buf[offset+i];
  offset += table_name_size;
  int32_t root_page_id = MACH_READ_INT32(buf + offset);
  offset += sizeof(int32_t);
  auto schema = (Schema*)heap->Allocate(sizeof(Schema));
  Schema::DeserializeFrom(buf + offset, schema, heap);
  offset += schema->GetSerializedSize();
  table_meta = TableMetadata::Create(table_id, table_name, root_page_id, schema, heap);
  return offset;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
