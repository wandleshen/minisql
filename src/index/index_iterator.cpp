#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator() {
  leaf_ = nullptr;
  bpm_ = nullptr;
  index_ = -1;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(
    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf, BufferPoolManager *bpm, int index) {
  leaf_ = leaf;
  bpm_ = bpm;
  index_ = index;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr)
    bpm_->UnpinPage(leaf_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (index_ + 1 < leaf_->GetSize()) {
    ++index_;
  } else {
    bpm_->UnpinPage(leaf_->GetPageId(), false);
    if (leaf_->GetNextPageId() != INVALID_PAGE_ID) {
      leaf_ = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>*>(bpm_->FetchPage(leaf_->GetNextPageId())->GetData());
      index_ = 0;
    } else {
      leaf_ = nullptr;
      index_ = -1;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return leaf_ == itr.leaf_ && index_ == itr.index_ && bpm_ == itr.bpm_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
