#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // replace with your own code
  int index = 1;
  while (index < GetSize() && comparator(array_[index].first, key) <= 0) {
    index++;
  }
  return array_[index-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  SetSize(2);
  array_[0].first = new_key;  // invalid in fact
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetParentPageId(INVALID_PAGE_ID);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
    //遍历查找
    for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == old_value) {
      for (int j = GetSize() - 1; j > i; j--) {
        array_[j + 1] = array_[j];  //拷贝
      }
      //插入键值对
      array_[i + 1].first = new_key;
      array_[i + 1].second = new_value;
      //修改大小
      SetSize(GetSize() + 1);
      return GetSize();
    }
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  auto page = buffer_pool_manager->FetchPage(GetPageId());
  if (page != nullptr) {
    page->WLatch();
    //设置大小为原来的一半
    if (GetSize() % 2) //奇数
      recipient->SetSize(GetSize()/2+1);
    else //偶数
      recipient->SetSize(GetSize()/2);
    SetSize(GetSize() / 2);
    //转移一半键值
    for (int i = 0; i < recipient->GetSize(); i++) {
      auto child_page = buffer_pool_manager->FetchPage(array_[i + GetSize()].second);
      if (child_page != nullptr) {
        child_page->WLatch();
        auto node = reinterpret_cast<BPlusTreeInternalPage *>(child_page->GetData());
        //链接父节点
        node->SetParentPageId(recipient->GetPageId());
        //转移键值
        recipient->array_[i] = array_[i + GetSize()];
        child_page->WUnlatch();
        buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
      }
    }
    recipient->SetParentPageId(GetParentPageId());
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetPageId(), true);
  }
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents' page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  auto page = buffer_pool_manager->FetchPage(GetPageId());
  if (page != nullptr) {
    page->WLatch();
    SetSize(size);
    //拷贝size个元素
    for (int i = 0; i < size; i++) {
      array_[i] = items[i];
      auto child_page = buffer_pool_manager->FetchPage(items[i].second);
      auto node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
      if (child_page != nullptr) {
        //更改父节点
        node->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(items[i].second, true);
      }
    }
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetPageId(), true);
  }
  page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page != nullptr) {
    page->WLatch();
    auto node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    node->SetKeyAt(node->ValueIndex(GetPageId()), items[0].first);
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  SetSize(GetSize() - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  ValueType only_child = array_[0].second;
  SetSize(0);
  return only_child;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  auto page = buffer_pool_manager->FetchPage(GetPageId());
  if (page != nullptr) {
    page->WLatch();
    auto size = recipient->GetSize();
    recipient->SetSize(GetSize() + recipient->GetSize());
    //全部转移
    for (int i = 0; i < GetSize(); i++) {
      //通过键值对找到该节点
      auto child_page = buffer_pool_manager->FetchPage(array_[i].second);
      child_page->WLatch();
      if (child_page != nullptr) {
        auto node = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
        //修改父节点指针
        node->SetParentPageId(recipient->GetPageId());
        child_page->WUnlatch();
        buffer_pool_manager->UnpinPage(array_[i].second, true);
      }
      recipient->array_[i + size] = array_[i];
    }
    //修改父节点指针
    recipient->SetParentPageId(GetParentPageId());
    SetSize(0);
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetPageId(), true);
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  auto page = buffer_pool_manager->FetchPage(GetPageId());
  if (page != nullptr) {
    page->WLatch();
    //完成相关设置
    recipient->SetSize(recipient->GetSize() + 1);
    //末元素追加
    recipient->array_[recipient->GetSize() - 1] = array_[0];
    recipient->SetParentPageId(GetParentPageId());
    SetSize(GetSize() - 1);
    //移出首个键值对
    for (int i = 0; i < GetSize(); i++) {
      array_[i] = array_[i + 1];
    }
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetPageId(), true);
  }
  if (GetParentPageId() != INVALID_PAGE_ID) {
    page = buffer_pool_manager->FetchPage(recipient->GetParentPageId());
    if (page != nullptr) {
      page->WLatch();
      auto node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
      //更新父节点的key
      node->SetKeyAt(node->ValueIndex(GetPageId()), array_[0].first);
      page->WUnlatch();
      buffer_pool_manager->UnpinPage(recipient->GetParentPageId(), true);
    }
    page = buffer_pool_manager->FetchPage(recipient->array_[recipient->GetSize() - 1].second);
    if (page != nullptr) {
      page->WLatch();
      auto node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
      //设置父节点
      node->SetParentPageId(recipient->GetPageId());
      page->WUnlatch();
      buffer_pool_manager->UnpinPage(recipient->array_[recipient->GetSize() - 1].second, true);
    }
  }
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto page = buffer_pool_manager->FetchPage(GetPageId());
  if (page != nullptr) {
    page->WLatch();
    SetSize(GetSize() + 1);
    array_[GetSize() - 1] = pair; //设置最后一个键值对
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetPageId(), true);
  }
  page = buffer_pool_manager->FetchPage(array_[GetSize() - 1].second);
  if (page != nullptr) {
    page->WLatch();
    auto node = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
    //更新
    node->SetParentPageId(GetPageId());
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(array_[GetSize() - 1].second, true);
  }
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  auto page = buffer_pool_manager->FetchPage(GetPageId());
  if (page != nullptr) {
    page->WLatch();
    recipient->SetSize(recipient->GetSize() + 1);
    //腾出空位
    for (int i = recipient->GetSize() - 1; i > 0; i--) {
      recipient->array_[i] = recipient->array_[i - 1];
    }
    //插入首个键值对
    recipient->array_[0] = array_[GetSize() - 1];
    recipient->SetParentPageId(GetParentPageId());
    SetSize(GetSize() - 1);
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetPageId(), true);
  }
  if (GetParentPageId() != INVALID_PAGE_ID) {
    page = buffer_pool_manager->FetchPage(recipient->GetParentPageId());
    if (page != nullptr) {
      page->WLatch();
      auto node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
      //检查更新key
      node->SetKeyAt(node->ValueIndex(recipient->GetPageId()), recipient->array_[0].first);
      page->WUnlatch();
      buffer_pool_manager->UnpinPage(recipient->GetParentPageId(), true);
    }
    page = buffer_pool_manager->FetchPage(recipient->array_[0].second);
    if (page != nullptr) {
      page->WLatch();
      auto node = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
      //设置父节点
      node->SetParentPageId(recipient->GetPageId());
      page->WUnlatch();
      buffer_pool_manager->UnpinPage(recipient->array_[0].second, true);
    }
  }
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto page = buffer_pool_manager->FetchPage(GetPageId());
  if (page != nullptr) {
    page->WLatch();
    SetSize(GetSize() + 1);
    //腾出首个位置
    for (int i = GetSize() - 1; i > 0; i--) {
      array_[i] = array_[i - 1];
    }
    //赋值
    array_[0] = pair;
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetPageId(), true);
  }
  page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page != nullptr) {
    page->WLatch();
    auto node = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
    //更新key
    node->SetKeyAt(node->ValueIndex(GetPageId()), array_[0].first);
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);
  }
  page = buffer_pool_manager->FetchPage(array_[0].second);
  if (page != nullptr) {
    page->WLatch();
    auto node = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
    //设置parent
    node->SetParentPageId(GetPageId());
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(array_[0].second, true);
  }
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
