#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  auto offset = sizeof(uint32_t);//加魔数所需要的字节
  std::string::size_type length = name_.length();
  MACH_WRITE_TO(std::string::size_type, buf+offset, length);
  offset += sizeof(std::string::size_type);
  MACH_WRITE_STRING(buf+offset, name_);
  offset += length;//加Column名称所需的字节
  MACH_WRITE_TO(TypeId, buf+offset, type_);
  offset += sizeof(TypeId);//加数据类型所需的字节
  MACH_WRITE_UINT32(buf+offset, len_);
  offset += sizeof(uint32_t);//
  MACH_WRITE_UINT32(buf+offset, table_ind_);
  offset += sizeof(uint32_t);//加表名所需的字节
  MACH_WRITE_TO(bool, buf+offset, nullable_);
  offset += sizeof(bool);//加判断是否为非空所需的字节
  MACH_WRITE_TO(bool, buf+offset, unique_);
  offset += sizeof(bool);//加判断是否为独一的所需的字节
  return offset;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return sizeof(uint32_t) + sizeof(std::string::size_type) + name_.length() + sizeof(TypeId) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(bool) + sizeof(bool);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
  uint32_t magic_num = MACH_READ_UINT32(buf);//获取Column中的魔数，判断Column是否正确
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Wrong magic number.");
  auto offset = sizeof(uint32_t);
  std::string::size_type length = MACH_READ_FROM(std::string::size_type, buf+offset);//反序列化魔数
  offset += sizeof(std::string::size_type);
  std::string name;//定义Cloumn的名称
  for (std::string::size_type i = 0; i < length; i++)//反序列化数据类型
    name += buf[offset+i];
  offset += length;
  TypeId type = MACH_READ_FROM(TypeId, buf+offset);
  offset += sizeof(TypeId);//反序列化column的类型id
  uint32_t len = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);
  uint32_t table_ind = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);//反序列化column在table中的id
  bool nullable = MACH_READ_FROM(bool, buf+offset);
  offset += sizeof(bool);//反序列化column是否为空的
  bool unique = MACH_READ_FROM(bool, buf+offset);
  offset += sizeof(bool);//反序列化是否column为独一的
  void *mem = heap->Allocate(sizeof(Column));
  if (type == TypeId::kTypeChar)//将Column中反序列化出的各种属性赋值到新的column
    column = new (mem)Column(name, type, len, table_ind, nullable, unique);
  else
    column = new (mem)Column(name, type, table_ind, nullable, unique);
  return offset;
}
