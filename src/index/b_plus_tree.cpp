#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  auto header_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  header_page->GetRootId(index_id_, &root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  if (root_page_id_ != INVALID_PAGE_ID) {
    auto root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    auto leaf = reinterpret_cast<LeafPage *>(FindLeafPage(root->KeyAt(0), true));
    last_page_id_ = leaf->GetPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  auto header_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  header_page->Delete(index_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  if (root_page_id_ == INVALID_PAGE_ID)
    return;
  // dfs
  list<page_id_t> stack;
  stack.emplace_back(root_page_id_);
  while (!stack.empty()) {
    auto page = *stack.begin();
    stack.pop_front();
    auto node = reinterpret_cast<InternalPage*>(buffer_pool_manager_
                    ->FetchPage(page)->GetData());
    if (node->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(page, false);
      buffer_pool_manager_->DeletePage(page);
      continue;
    }
    for (int i = 0; i < node->GetSize(); ++i) {
      if (node->ValueAt(i) != INVALID_PAGE_ID)
        stack.emplace_back(node->ValueAt(i));
    }
    buffer_pool_manager_->UnpinPage(page, false);
    buffer_pool_manager_->DeletePage(page);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  if(root_page_id_  == INVALID_PAGE_ID) return true;
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query 点查询
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, LeafPage* leaf, int& index, Transaction *transaction) {
  if(this->IsEmpty()) return false; //空树
  if (!leaf)
    leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, false));
  //找到了
  ValueType value;
  if(leaf->Lookup(key, value, comparator_, index))
  {
    //这里有点疑惑？为什么传的是vector型而不是直接是ValueType...
    //先直接push_back吧
    result.push_back(value);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  return false;
}


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty())
  {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_id;
  Page *root = buffer_pool_manager_->NewPage(root_id);
  //allocate successfully
  if(root)
  {
    auto *leaf = reinterpret_cast<LeafPage *>(root->GetData());
    leaf->Init(root_id, INVALID_PAGE_ID, leaf_max_size_);
    root_page_id_ = root_id;
    last_page_id_ = root_page_id_;
    UpdateRootPageId(true);
    int index = -1;
    leaf->Insert(key, value, comparator_, index);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
  //failed
  //throw std::bad_alloc();
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  std::vector<ValueType> result;
  //已经存在
  auto leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, false)->GetData());
  int index = 0;
  if(GetValue(key, result, leaf, index, transaction)) {
    return false;
  }
  //fetch the page
//  LeafPage * leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, false)->GetData());
  leaf->Insert(key, value, comparator_, index);
  last_page_id_ = leaf->GetPageId();
  //存疑
  if(leaf->GetSize() > leaf->GetMaxSize())
  {
    LeafPage *new_leaf = Split(leaf);
    last_page_id_ = new_leaf->GetPageId();
    InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if(!new_page)
    throw std::bad_alloc();
  //是叶子
  if(node->IsLeafPage())
  {
    //转换
    LeafPage * old_leaf_page = reinterpret_cast<LeafPage *>(node);
    LeafPage * new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_leaf_page->Init(new_page_id, old_leaf_page->GetParentPageId(), leaf_max_size_);
    //转移一半
    old_leaf_page->MoveHalfTo(new_leaf_page);
    //指针横向链接
    new_leaf_page->SetNextPageId(old_leaf_page->GetNextPageId());
    old_leaf_page->SetNextPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    buffer_pool_manager_->UnpinPage(old_leaf_page->GetPageId(), true);
    return reinterpret_cast<N *>(new_leaf_page);
  }
  else
  {
    InternalPage *new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    InternalPage *old_internal_page = reinterpret_cast<InternalPage *>(node);
    new_internal_page->Init(new_page_id, old_internal_page->GetParentPageId(), internal_max_size_);
    //MoveHalfTo这个函数应该是要转移节点中的父节点指向的~
    old_internal_page->MoveHalfTo(new_internal_page, buffer_pool_manager_);
    return reinterpret_cast<N *>(new_internal_page);
  }
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  //如果是根节点
  if (old_node->IsRootPage()) {
    // 创建新的根节点要更新root_page_id_和header_page
    //更新新的root_page_id
    Page *new_page = buffer_pool_manager_->NewPage(root_page_id_);
    //更新header_page
    //是更新不是插入！！！只有新建树的时候是true
    UpdateRootPageId(false);
    //新建一个根internal page
    InternalPage *new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    //赋值
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    //更新父节点
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    //标记is dirty
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    //不是root
    page_id_t parent_page_id = old_node->GetParentPageId();
    //获取父节点
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
    //插入new node
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page_id);
    // 父节点到达max_size+1需要进行分裂
    if (parent_page->GetSize() > parent_page->GetMaxSize()) {
      //父节点进行分裂
      InternalPage *parent_page_2 = Split(parent_page);
      //递归向上连接
      InsertIntoParent(parent_page, parent_page_2->KeyAt(0), parent_page_2);
      //连接结束之后unpin page
      buffer_pool_manager_->UnpinPage(parent_page_2->GetPageId(), true);
    }
    //unpin page 标记is dirty
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  //获取该叶
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, false)->GetData());
  if (leaf_page->KeyIndex(key, comparator_) == 0 && leaf_page->GetParentPageId() != INVALID_PAGE_ID) {
    auto tmp_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId())->GetData());
    auto index = tmp_page->ValueIndex(leaf_page->GetPageId());
    tmp_page->SetKeyAt(index, leaf_page->KeyAt(1));
    buffer_pool_manager_->UnpinPage(leaf_page->GetParentPageId(), true);
    auto page_id = leaf_page->GetPageId();
    while (tmp_page->ValueIndex(page_id) == 0 && tmp_page->GetParentPageId() != INVALID_PAGE_ID) {
      auto node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(tmp_page->GetParentPageId())->GetData());
      index = node->ValueIndex(tmp_page->GetPageId());
      node->SetKeyAt(index, tmp_page->KeyAt(0));
      page_id = tmp_page->GetPageId();
      tmp_page = node;
    }
  }
  //删除记录
  leaf_page->RemoveAndDeleteRecord(key, comparator_);
  //过少则合并
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    CoalesceOrRedistribute(leaf_page, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
//node 中的节点不满足要求，需要与相邻节点合并
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  //是根：无需合并or调整
  if (node->IsRootPage())
    return AdjustRoot(node);
  //get the parent
  InternalPage * parent = reinterpret_cast<InternalPage *>
      (buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData()); //need to unpin
  int this_index = parent->ValueIndex(node->GetPageId());
  page_id_t sibling_id;
  N* sibling;
  //the first
  if(!this_index)
    sibling_id = parent->ValueAt(this_index + 1); // the right sibling
  else
    sibling_id = parent->ValueAt(this_index - 1); // the left sibling
  //get the sibling and pin it
  sibling = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(sibling_id)->GetData()); // need to unpin
  //merge
  if (node->GetSize() + sibling->GetSize() <= node->GetMaxSize()) {
    // 将右边节点合并到左边节点上
    //默认node是右边节点
    if (!this_index) { //交换
      N *temp = node;
      node = sibling;
      sibling = temp;
    }
    // index是右边节点所在的index
    int index = parent->ValueIndex(node->GetPageId());
    //进行合并
    Coalesce(&sibling, &node, &parent, index, transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    return true;
  }
  // index为underflow的节点在父节点中的index
  //node : 需要
  int index = parent->ValueIndex(node->GetPageId());
  //重新分配
  Redistribute(sibling, node, index);
  //unpin
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
//the level format : ... - **neighbor_node - **node - ...
//delete **node
//index : 右边节点(**Node)所在的的index
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if ((*node)->IsLeafPage()) {
    LeafPage *this_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *neighbor = reinterpret_cast<LeafPage *>(*neighbor_node);
    this_node->MoveAllTo(neighbor);
    neighbor->SetNextPageId(this_node->GetNextPageId());
  } else {
    InternalPage *this_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *neighbor = reinterpret_cast<InternalPage *>(*neighbor_node);
    this_node->MoveAllTo(neighbor, (*parent)->KeyAt(index), buffer_pool_manager_);
  }
  //delete **node
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(),true);
  buffer_pool_manager_->DeletePage((*node)->GetPageId());
  (*parent)->Remove(index);
  //continue
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute((*parent), transaction);
  }
  //end : successfully delete
  return true;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
* 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
//不能传page类型...传了就寄
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  //find parent node
  InternalPage *parent =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  //node - neighbor_node - ...
  if (index == 0) {
    // 右边节点在父节点中对应的index
    //actually it is 1
    index = parent->ValueIndex(neighbor_node->GetPageId());
    //leaf page
    if (neighbor_node->IsLeafPage()) {
      //type converse
      LeafPage *neighbor = reinterpret_cast<LeafPage *>(neighbor_node);
      LeafPage *this_node = reinterpret_cast<LeafPage *>(node);
      //remove
      neighbor->MoveFirstToEndOf(this_node);
      //update parent key
      parent->SetKeyAt(index, neighbor->KeyAt(0));
    }
    //internal page
    else {
      InternalPage *neighbor = reinterpret_cast<InternalPage *>(neighbor_node);
      InternalPage *this_node = reinterpret_cast<InternalPage *>(node);
      neighbor->MoveFirstToEndOf(this_node, parent->KeyAt(index), buffer_pool_manager_);
    }
  }
  //... - neighbor_node - node - ...
  else {
    if (neighbor_node->IsLeafPage()) {
      LeafPage *neighbor = reinterpret_cast<LeafPage *>(neighbor_node);
      LeafPage *this_node = reinterpret_cast<LeafPage *>(node);
      neighbor->MoveLastToFrontOf(this_node);
      parent->SetKeyAt(index, this_node->KeyAt(0));
    } else {
      InternalPage *neighbor = reinterpret_cast<InternalPage *>(neighbor_node);
      InternalPage *this_node = reinterpret_cast<InternalPage *>(node);
      neighbor->MoveLastToFrontOf(this_node, parent->KeyAt(index), buffer_pool_manager_);
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within **coalesceOrRedistribute()** method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // case 1: 删除元素，此时的根节点只有1个child， 需要释放根节点
  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
    InternalPage *root_page = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_id = root_page->RemoveAndReturnOnlyChild();
    // 获取新的根
    InternalPage *new_root_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(new_root_id)->GetData());
    //父节点设为null
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    //更新root
    root_page_id_ = new_root_id;
    //更新roots page
    UpdateRootPageId(false);
    //unpin
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    return true;
  }
  // case 2: when you delete the last element in whole b+ tree
  if (old_root_node->IsLeafPage() && !old_root_node->GetSize()) {
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(root_page_id_);
    //设为invalid
    root_page_id_ = INVALID_PAGE_ID;
    // 更新
    UpdateRootPageId(false);
    return true;
  }
  //无事发生
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  auto root = reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  auto key = root->KeyAt(0);
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  auto leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, true));
  return INDEXITERATOR_TYPE(leaf, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, false));
  return INDEXITERATOR_TYPE(leaf, buffer_pool_manager_, leaf->KeyIndex(key, comparator_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_, -1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if (IsEmpty()) return nullptr;
  auto leaf = reinterpret_cast<LeafPage*>(buffer_pool_manager_->FetchPage(last_page_id_)->GetData());
  if (!leftMost && leaf->IsLast(key, comparator_))
    return reinterpret_cast<Page * >(leaf);
  page_id_t next_page_id = root_page_id_;
  //获取该页
  InternalPage *page = reinterpret_cast<InternalPage *>
      (buffer_pool_manager_->FetchPage(next_page_id)->GetData());
  while(!page->IsLeafPage())
  {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
    // 警钟长鸣！不要随便混用 LeafPage 和 InternalPage 的函数！
    if (page->IsLeafPage()) {
      break;
    }
    if(leftMost)
      next_page_id = page->ValueAt(0); //find the left most leaf page
    else
      next_page_id = page->Lookup(key, comparator_);

  }
  return reinterpret_cast<Page *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  IndexRootsPage *header_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (insert_record)
    // 1:新建一个root
    header_page->Insert(index_id_, root_page_id_);
  else
    //0:更新某个root
    header_page->Update(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
