#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"

enum class Event_type { undef, error, ping, update, snapshot };

class Processed_data {
 public:
  Event_type event = Event_type::undef;
  unsigned long time = 0;
  std::vector<std::pair<double, int>> asks;
  std::vector<std::pair<double, int>> bids;
  const std::string members[3] = {"ch", "ts", "tick"};
  const std::string tick_members[3] = {"asks", "bids", "event"};

  Processed_data(const std::string& str) {
    rapidjson::Document document;
    document.Parse(str.c_str() + str.find("{"));

    auto [ev, msg] = check_data(document);

    std::cerr << msg << " " << str << std::endl;

    switch (ev) {
      case Event_type::error: {
        event = Event_type::error;
        return;
      }
      case Event_type::ping: {
        event = Event_type::ping;
        return;
      }
      case Event_type::snapshot: {
        event = Event_type::snapshot;
        fill_data(document);
        return;
      }
      case Event_type::update: {
        event = Event_type::update;
        fill_data(document);
        return;
      }
      default:
        event = Event_type::undef;
        return;
    }
  }

  std::pair<Event_type, std::string> check_data(
      const rapidjson::Document& document) const {
    if (document.HasParseError())
      return {Event_type::error,
              "error: " + std::string(rapidjson::GetParseError_En(
                              document.GetParseError()))};

    if (document.HasMember("ping")) return {Event_type::ping, "ping"};

    for (const auto& v : members) {
      if (document.HasMember(v.c_str()))
        continue;
      else
        return {Event_type::error, "error: no member: " + std::string(v)};
    }

    for (const auto& v : tick_members) {
      if (document["tick"].HasMember(v.c_str()))
        continue;
      else
        return {Event_type::error, "error: no member: " + std::string(v)};
    }

    for (const auto& v : document["tick"]["asks"].GetArray())
      if (!v[1].IsInt() && !v[0].IsDouble())
        return {Event_type::error, "value error"};

    for (const auto& v : document["tick"]["asks"].GetArray())
      if (!v[1].IsInt() && !v[0].IsDouble())
        return {Event_type::error, "value error"};

    if (document["tick"]["event"] == "snapshot")
      return {Event_type::snapshot, "success"};
    else
      return {Event_type::update, "success: "};
  }

  void fill_data(const rapidjson::Document& document) {
    time = document["ts"].GetUint64();

    for (const auto& v : document["tick"]["asks"].GetArray()) {
      if (v[1].GetInt() == 0) continue;

      asks.emplace_back(v[0].GetDouble(), v[1].GetInt());
    }

    for (const auto& v : document["tick"]["bids"].GetArray()) {
      if (v[1].GetInt() == 0) continue;

      bids.emplace_back(v[0].GetDouble(), v[1].GetInt());
    }
  }
};

class Limit_order_book {
 public:
  Limit_order_book() = default;
  ~Limit_order_book() = default;

  void set_snapshot(const Processed_data& respond) {
    time = respond.time;

    asks.clear();
    bids.clear();

    for (const auto& v : respond.asks) {
      if (v.first == 0) continue;

      asks[v.first] = v.second;
    }

    for (const auto& v : respond.bids) {
      if (v.first == 0) continue;

      bids[v.first] = v.second;
    }
  }

  void update_snapshot(const Processed_data& respond) {
    time = respond.time;

    for (const auto& v : respond.asks) {
      if (v.first == 0) {
        asks.erase(v.first);
        continue;
      }
      asks[v.first] = v.second;
    }

    for (const auto& v : respond.bids) {
      if (v.first == 0) {
        bids.erase(v.first);
        continue;
      }
      bids[v.first] = v.second;
    }
  }

  std::pair<double, int> get_best_ask() const {
    return {asks.cbegin()->first, asks.cbegin()->second};
  }

  std::pair<double, int> get_best_bid() const {
    return {bids.cbegin()->first, bids.cbegin()->second};
  }

  unsigned long get_time() const { return time; }

 private:
  unsigned long time = 0;
  std::map<double, int, std::less<>> asks;
  std::map<double, int, std::greater<>> bids;
};

int main(int argc, char** argv) {
  std::ifstream input(argv[1]);
  std::ofstream output(argv[2]);

  if (!input.is_open() || !output.is_open()) return 1;

  Limit_order_book l;
  std::string s = "";
  std::vector<Processed_data> updates;

  std::chrono::duration<double, std::nano> summ_update_time;
  std::chrono::_V2::steady_clock::time_point start;
  std::chrono::_V2::steady_clock::time_point end;

  while (std::getline(input, s)) {
    Processed_data ev = Processed_data(s);

    if (ev.event == Event_type::snapshot) {
      l.set_snapshot(ev);

      auto [ask_price, ask_amount] = l.get_best_ask();
      auto [bid_price, bid_amount] = l.get_best_bid();

      output << std::fixed << std::setprecision(2) << "{" << l.get_time()
             << "}, {" << bid_price << "}, {" << bid_amount << "}, {"
             << ask_price << "}, {" << ask_amount << "}" << std::endl;

    } else if (ev.event == Event_type::update) {
      updates.push_back(std::move(ev));
    }
  }

  for (const auto& v : updates) {
    start = std::chrono::steady_clock::now();

    l.update_snapshot(v);

    end = std::chrono::steady_clock::now();
    summ_update_time += end - start;

    auto [ask_price, ask_amount] = l.get_best_ask();
    auto [bid_price, bid_amount] = l.get_best_bid();

    output << std::fixed << std::setprecision(2) << "{" << l.get_time()
           << "}, {" << bid_price << "}, {" << bid_amount << "}, {" << ask_price
           << "}, {" << ask_amount << "}" << std::endl;
  }

  std::cout << "average update time: "
            << summ_update_time.count() / updates.size() << " nanoseconds"
            << std::endl;

  return 0;
}