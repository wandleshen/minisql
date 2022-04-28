#ifndef MINISQL_CLOCK_REPLACER_H
#define MINISQL_CLOCK_REPLACER_H

#include <list>
#include <map>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

class ClockReplacer : public Replacer {
 public:
  explicit ClockReplacer(size_t num_pages);

  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // add your own private member variables here
  mutex lock_;
  vector<pair<frame_id_t, bool>> clock_vec_;
  int clock_hand_;
  map<frame_id_t, vector<pair<frame_id_t, bool>>::iterator> list_map_;
};

#endif  // MINISQL_CLOCK_REPLACER_H
