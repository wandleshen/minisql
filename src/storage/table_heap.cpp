#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
//  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
//  while (true) {
//    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
//      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
//      page->WUnlatch();
//      return true;
//    }
//    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
//    page->WUnlatch();
//    if (page->GetNextPageId() == INVALID_PAGE_ID)
//      break;
//    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
//    page->WLatch();
//  }
  if (!max_free_page_.empty()) {//找到最前的page，当最前的page不为空时，直接插入
    auto top_page = max_free_page_.top();
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(top_page.page_id_));
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {//当插入page成功后返回true
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      page->WUnlatch();
      top_page.size_ -= 1;//top_page的大小减一
      max_free_page_.pop();
      if (top_page.size_ > 0)
        max_free_page_.push(top_page);//当top_page的大小大于0时将top_page压入max_free_page
      return true;
    }
}
  //当top_page为空时新建一个top_page并插入
  page_id_t page_id;
  //新建一个page
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
  if (!new_page)
    return false;
  //page的id为最后的page_id
  new_page->Init(page_id, last_page_id_, log_manager_, txn);
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_));
  page->SetNextPageId(new_page->GetPageId());//new_page更新为last_page
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  //将tuple插入到新的page中
  bool ans = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  max_free_page_.push(MaxHeapNode(page_id, PAGE_SIZE - 1));
  last_page_id_ = page_id;
  return ans;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  Row* old_row = new Row(rid);
  if (!GetTuple(old_row, txn))//将原来的tuple复制到old_row
    return false;
  int err_code;
  page->WLatch();
  bool flag = page->UpdateTuple(row, old_row, schema_, err_code, txn, lock_manager_, log_manager_);//将page中的old_row更新为新的row
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  // update for extra requests
  if (!flag && err_code == 1) //当更新不正确时删除更新
  {
    flag = InsertTuple(row, txn);
    if (flag) {
      MarkDelete(rid, txn);//将row的标记更新为删除
      ApplyDelete(rid, txn);//物理层面上删除
    }
    return flag;
  }
  return flag;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));//找到当前page
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);//将当前的page中的元组删除
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);//更新page
  vector<MaxHeapNode> nodes;
}

void TableHeap::RecreateQueue() {
  priority_queue<MaxHeapNode> new_queue;
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));
  while (true) {
    auto size = PAGE_SIZE;
    auto r_id = new RowId;
    if (!page->GetFirstTupleRid(r_id)) {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      break;
    }
    while (page->GetNextTupleRid(*r_id, r_id)) {
      size--;
    }
    if (size > 0)
      new_queue.push(MaxHeapNode(page->GetPageId(), size));
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    if (page->GetNextPageId() == INVALID_PAGE_ID)
      break;
    page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  }
  max_free_page_ = new_queue;
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  while (page->GetNextPageId() != INVALID_PAGE_ID) //page未删除完全时持续删除
  {
    auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));//next_page指向下一个page
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(page->GetPageId());//删除当前的page
    page = next_page;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  buffer_pool_manager_->DeletePage(page->GetPageId());//删除最后的page
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));//得到row所在的page的id
  assert(page != nullptr);
  page->RLatch();
  bool result = page->GetTuple(row, schema_, txn, lock_manager_);//从page中获得row的值
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return result;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  //得到第一页
  TablePage* first = (reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_)));
  //建立一个指向行的指针
  auto first_id = new RowId;
  //直到遇见1才停
  //bool GetFirstTupleRid(RowId *first_rid);
  while (true)
  {
    if (first->GetFirstTupleRid(first_id)) break; /*找到了第一个*/
    if (first->GetNextPageId() == INVALID_PAGE_ID) /*已经是最后一页了，并且没有找到第一个元组*/
    {
      delete first_id;  /*释放空间*/
      return TableIterator(nullptr, this);  /*返回null*/
    }
    //更新page
    first = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first->GetNextPageId()));
  }
  Row* r = new Row(*first_id);  // update: row 需要通过 GetTuple 得到数据
  GetTuple(r, txn);
  //构建迭代器
  return TableIterator(r, this);}  // update: 返回一个拷贝构造
//  //释放空间
//  delete first_id;
//  //返回迭代器
//  return ite;}

TableIterator TableHeap::End() {
  //flag: 行指针为空
  return TableIterator(nullptr, this);}
