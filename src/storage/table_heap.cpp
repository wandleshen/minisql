#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  while (true) {
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      page->WUnlatch();
      return true;
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page->WUnlatch();
    if (page->GetNextPageId() == INVALID_PAGE_ID)
      break;
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    page->WLatch();
  }
  page_id_t page_id;
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
  if (!new_page)
    return false;
  new_page->Init(page_id, page->GetPageId(), log_manager_, txn);
  return new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
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

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  Row* old_row = new Row(rid);
  if (!GetTuple(old_row, txn))
    return false;
  int err_code;
  page->WLatch();
  bool flag = page->UpdateTuple(row, old_row, schema_, err_code, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  // update for extra requests
  if (!flag && err_code == 1) {
    Row* new_row = (Row*)&row;
    flag = InsertTuple(*new_row, txn);
    if (flag) {
      MarkDelete(rid, txn);
      ApplyDelete(rid, txn);
    }
    return flag;
  }
  return flag;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
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
  while (page->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(page->GetPageId());
    page = next_page;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  buffer_pool_manager_->DeletePage(page->GetPageId());
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  assert(page != nullptr);
  page->RLatch();
  bool result = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return result;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  //得到第一页
  TablePage* first = (reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_)));
  //建立一个指向行的指针
  RowId* first_id = new RowId;
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
