#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t offset=0;  // offset相当于内存往前推进的大小
  MACH_WRITE_UINT32(buf,TABLE_METADATA_MAGIC_NUM);  // 将魔数写入内存
  offset+=sizeof(uint32_t);   // offset加上一个整数所占大小,即sizeof(uint32_t)
  MACH_WRITE_UINT32(buf+offset,table_id_);  // 继续将table_id_写入内存
  offset+=sizeof(uint32_t);
  MACH_WRITE_TO(size_t, buf+offset, table_name_.length()); //记录表名的长度
  offset+=sizeof(size_t);
  MACH_WRITE_STRING(buf+offset, table_name_); // 再将表名写入内存
  offset+=table_name_.length();
  MACH_WRITE_TO(page_id_t,buf+offset,root_page_id_);
  offset+=sizeof(int32_t);
  offset+=schema_->SerializeTo(buf+offset);
  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t size=0;
  size=2*sizeof(uint32_t)+sizeof(size_t)+table_name_.length()+sizeof(int32_t);
  size+=schema_->GetSerializedSize();
  return size;  // 根据序列化函数offset依次的变化累加即可
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  if (table_meta != nullptr) {
    LOG(WARNING) << "Pointer to table_meta is not null." << std::endl;
    return 0;
  }
  uint32_t offset=0;
  uint32_t magic_num=MACH_READ_UINT32(buf);
  if(magic_num!=TABLE_METADATA_MAGIC_NUM) // 检查魔数
  {
    LOG(WARNING) << "MAGIC_NUM is wrong." << std::endl;
    return 0;
  }
  offset+=sizeof(uint32_t);
  table_id_t table_id=MACH_READ_UINT32(buf);  //读出table_id
  offset+=sizeof(uint32_t);
  size_t len=MACH_READ_FROM(size_t,buf);  //读出长度
  offset+=sizeof(size_t);
  std::string table_name;
  char* name=NULL;
  memcpy(name,buf+offset,len); //name string
  table_name=name;
  offset+=len;
  page_id_t root_pageid=MACH_READ_INT32(buf+offset);
  offset+=sizeof(int32_t);
  Schema* schema=nullptr;
  offset+=schema->DeserializeFrom(buf+offset,schema,heap);
  table_meta=ALLOC_P(heap, TableMetadata)(table_id,table_name,root_pageid,schema);
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
