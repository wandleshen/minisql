#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  auto offset = sizeof(uint32_t);//加魔数所需的字节
  MACH_WRITE_UINT32(buf+offset, columns_.size());
  offset += sizeof(uint32_t);
  for (auto &col : columns_) {
    col->SerializeTo(buf+offset);
    offset += col->GetSerializedSize();//加Schema中每个Column所需的字节
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t size = sizeof(uint32_t) * 2;
  for (auto &col : columns_) {
    size += col->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  ASSERT(schema != nullptr, "Pointer to schema is not null in column deserialize.");//获取schema中的魔数，判断schema是否正确
  uint32_t magic_num = MACH_READ_UINT32(buf);//反序列化魔数
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Wrong magic number.");
  auto offset = sizeof(uint32_t);
  uint32_t num_cols = MACH_READ_UINT32(buf+offset);//获取schema中的columa数并加所需的字节
  offset += sizeof(uint32_t);
  std::vector<Column *> columns;
  for (uint32_t i = 0; i < num_cols; i++) {
    Column *col = nullptr;
    offset += Column::DeserializeFrom(buf+offset, col, heap);//反序列化schema中的column
    columns.emplace_back(col);
  }
  void* schema_ptr = heap->Allocate(sizeof(Schema));
  schema = new (schema_ptr) Schema(columns);//将反序列化出的column赋值到新的schema
  return offset;
}
