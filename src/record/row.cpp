#include "record/row.h"

using namespace std;
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
//  MACH_WRITE_TO(RowId, buf, rid_);
//  uint32_t offset = sizeof(RowId);
  uint32_t offset = 0;
  vector<bool> bits;
  for (auto field : fields_) {
    bits.emplace_back() = field->IsNull();//将Row中空的field通过位图的方式进行记录
  }
  for (auto i : bits) {
    MACH_WRITE_TO(bool, buf + offset, i);//将Row中空的field所推进的字节加到buf中
    offset += sizeof(bool);
  }
  for (auto field : fields_) {
    if (!field->IsNull()) {
      field->SerializeTo(buf + offset);
      offset += field->GetSerializedSize();//将Row中非空的field进行序列化
    }
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
//  rid_ = MACH_READ_FROM(RowId, buf);
//  uint32_t offset = sizeof(RowId);
  uint32_t offset = 0;
  vector<bool> bits;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    bits.push_back(MACH_READ_FROM(bool, buf + offset));//从buf中反序列化的空的field，
    offset += sizeof(bool);//给offset加上需要的字节
  }
  for (unsigned long i = 0; i < bits.size(); i++) {
    void* tmp = heap_->Allocate(sizeof(Field));
    auto* field_tmp = new(tmp)Field(schema->GetColumn(i)->GetType());//得到所要反序列化的数据类型
    Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &field_tmp, bits[i], heap_);//将field反序列化
    fields_.push_back(field_tmp);
    offset += fields_.back()->GetSerializedSize();//给offset加上所需要字节
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  //uint32_t size = sizeof(RowId);
  uint32_t size = 0;
  for (auto field : fields_) {
    if (!field->IsNull())
      size += field->GetSerializedSize();//加上每个非空field的字节
  }
  size += fields_.size() * sizeof(bool);
  return size;
}
