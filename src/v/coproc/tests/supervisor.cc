// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "coproc/tests/supervisor.h"

#include "coproc/logger.h"
#include "coproc/tests/utils.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "storage/record_batch_builder.h"

#include <type_traits>

namespace coproc {

ss::future<> supervisor::invoke_coprocessor(
  const model::ntp& ntp,
  const script_id id,
  ss::circular_buffer<model::record_batch>&& batches,
  std::vector<process_batch_reply::data>& rs) {
    auto found = _coprocessors.local().find(id);
    if (found == _coprocessors.local().end()) {
        vlog(coproclog.warn, "Script id: {} not found", id);
        return ss::now();
    }
    auto& copro = found->second;
    return copro->apply(ntp.tp.topic, std::move(batches))
      .then([this, id, ntp, &rs](coprocessor::result rmap) {
          if (rmap.empty()) {
              // Must send at least one response so server can update the offset
              // This can be seen as a type of ack
              // TODO(rob) Maybe in this case we can adjust the type system,
              // possibly an optional response
              rs.emplace_back(process_batch_reply::data{
                .id = id,
                .ntp = ntp,
                .reader = model::make_memory_record_batch_reader(
                  model::record_batch_reader::data_t())});
              return ss::now();
          }
          return ss::do_with(
            std::move(rmap), [&rs, id, ntp](coprocessor::result& rmap) {
                return ss::do_for_each(
                  rmap, [&rs, id, ntp](coprocessor::result::value_type& p) {
                      rs.emplace_back(process_batch_reply::data{
                        .id = id,
                        .ntp = model::ntp(
                          ntp.ns,
                          to_materialized_topic(ntp.tp.topic, p.first),
                          ntp.tp.partition),
                        .reader = model::make_memory_record_batch_reader(
                          std::move(p.second))});
                  });
            });
      });
}

ss::future<std::vector<process_batch_reply::data>>
supervisor::invoke_coprocessors(process_batch_request::data d) {
    return model::consume_reader_to_memory(
             std::move(d.reader), model::no_timeout)
      .then([this, script_ids = std::move(d.ids), ntp = std::move(d.ntp)](
              model::record_batch_reader::data_t rbr) {
          std::vector<process_batch_reply::data> results;
          return ss::do_with(
            std::move(results),
            std::move(rbr),
            std::move(script_ids),
            std::move(ntp),
            [this](
              auto& results,
              const model::record_batch_reader::data_t& rbr,
              const std::vector<script_id>& sids,
              const model::ntp& ntp) {
                return ss::do_for_each(
                         sids,
                         [this, &ntp, &rbr, &results](script_id sid) {
                             return copy_batch(rbr).then(
                               [this, sid, &ntp, &rbr, &results](
                                 model::record_batch_reader::data_t batch) {
                                   return invoke_coprocessor(
                                     ntp, sid, std::move(batch), results);
                               });
                         })
                  .then([&results]() { return std::move(results); });
            });
      });
}

ss::future<process_batch_reply>
supervisor::process_batch(process_batch_request&& r, rpc::streaming_context&) {
    return ss::with_gate(_gate, [this, r = std::move(r)]() mutable {
        if (r.reqs.empty()) {
            vlog(coproclog.error, "Error with redpanda, request is of 0 size");
            /// TODO(rob) check if this is ok
            return ss::make_ready_future<process_batch_reply>();
        }
        return ss::do_with(std::move(r), [this](process_batch_request& r) {
            return ssx::async_flat_transform(
                     r.reqs.begin(),
                     r.reqs.end(),
                     [this](process_batch_request::data& d) {
                         return invoke_coprocessors(std::move(d));
                     })
              .then([](std::vector<process_batch_reply::data> replies) {
                  return process_batch_reply{.resps = std::move(replies)};
              });
        });
    });
}

} // namespace coproc
