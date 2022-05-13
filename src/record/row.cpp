#include "record/row.h"

using namespace std;
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  MACH_WRITE_TO(RowId, buf, rid_);
  uint32_t offset = sizeof(RowId);
  vector<bool> bits;
  for (auto field : fields_) {
    bits.emplace_back() = field->IsNull();
  }
  for (auto i : bits) {
    MACH_WRITE_TO(bool, buf + offset, i);
    offset += sizeof(bool);
  }
  for (auto field : fields_) {
    if (!field->IsNull()) {
      field->SerializeTo(buf + offset);
      offset += field->GetSerializedSize();
    }
  }
  return GetSerializedSize(schema);
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  rid_ = MACH_READ_FROM(RowId, buf);
  uint32_t offset = sizeof(RowId);
  vector<bool> bits;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    bits.emplace_back(MACH_READ_FROM(bool, buf + offset));
    offset += sizeof(bool);
  }
  for (unsigned long i = 0; i < bits.size(); i++) {
    void* tmp = heap_->Allocate(sizeof(Field));
    auto* field_tmp = new(tmp)Field(schema->GetColumn(i)->GetType());
    Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &field_tmp, bits[i], heap_);
    fields_.emplace_back(field_tmp);
    offset += fields_.back()->GetSerializedSize();
  }
  return GetSerializedSize(schema);
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t size = sizeof(RowId);
  for (auto field : fields_) {
    if (!field->IsNull())
      size += field->GetSerializedSize();
  }
  size += fields_.size() * sizeof(bool);
  return size;
}
