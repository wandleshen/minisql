#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM); //写入魔数
  auto offset = sizeof(uint32_t); //offset加上一个整数所占大小,即sizeof(uint32_t)
  MACH_WRITE_UINT32(buf + offset, index_id_); //写入index id
  offset += sizeof(uint32_t); //offset加上整数所占字节大小
  MACH_WRITE_UINT32(buf + offset, index_name_.size());  //写入index名称字符串的长度
  offset += sizeof(uint32_t); //offset加上整数所占字节大小
  memcpy(buf + offset, index_name_.c_str(), index_name_.size());
  offset += index_name_.size(); //offset加上名称字符串长度
  MACH_WRITE_UINT32(buf + offset, table_id_);
  offset += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + offset, key_map_.size());
  offset += sizeof(uint32_t);
  for (auto key_id : key_map_) {
    MACH_WRITE_UINT32(buf + offset, key_id);
    offset += sizeof(uint32_t);
  }
  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return sizeof(uint32_t) * 5 + index_name_.size() + sizeof(uint32_t) * key_map_.size(); //与上述过程一致
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  ASSERT(MACH_READ_UINT32(buf) == INDEX_METADATA_MAGIC_NUM, "invalid index metadata");  // 检查魔数
  auto offset = sizeof(uint32_t);
  uint32_t index_id = MACH_READ_UINT32(buf + offset); //读出index id
  offset += sizeof(uint32_t);
  uint32_t index_name_size = MACH_READ_UINT32(buf + offset);  //读出索引名称字符串长度
  offset += sizeof(uint32_t);
  string index_name(buf + offset, index_name_size); //读出索引名称
  offset += index_name_size;
  uint32_t table_id = MACH_READ_UINT32(buf + offset); //读出table id
  offset += sizeof(uint32_t);
  uint32_t key_map_size = MACH_READ_UINT32(buf + offset); 
  offset += sizeof(uint32_t);
  vector<uint32_t> key_map;
  for (uint32_t i = 0; i < key_map_size; i++) {
    uint32_t key_id = MACH_READ_UINT32(buf + offset);
    offset += sizeof(uint32_t);
    key_map.emplace_back(key_id);
  }
  index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map, heap);
  return offset;
}
