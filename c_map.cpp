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

enum class Event { undef, error, ping, update, snapshot };

class Limit_order_book {
 public:
  Limit_order_book() = default;
  ~Limit_order_book() = default;

  std::pair<Event, rapidjson::Document> process_data(const std::string& str) {
    rapidjson::Document document;
    document.Parse(str.c_str() + str.find("{"));

    auto [ev, msg] = check_data(document);

    std::cerr << msg << " " << str << std::endl;

    return {ev, std::move(document)};
  }

  std::pair<Event, std::string> check_data(
      const rapidjson::Document& document) const {
    if (document.HasParseError())
      return {Event::error, "error: " + std::string(rapidjson::GetParseError_En(
                                            document.GetParseError()))};

    if (document.HasMember("ping")) return {Event::ping, "ping"};

    for (const auto& v : members) {
      if (document.HasMember(v.c_str()))
        continue;
      else
        return {Event::error, "error: no member: " + std::string(v)};
    }

    for (const auto& v : tick_members) {
      if (document["tick"].HasMember(v.c_str()))
        continue;
      else
        return {Event::error, "error: no member: " + std::string(v)};
    }

    for (const auto& v : document["tick"]["asks"].GetArray())
      if (!v[1].IsInt() && !v[0].IsDouble())
        return {Event::error, "value error"};

    for (const auto& v : document["tick"]["asks"].GetArray())
      if (!v[1].IsInt() && !v[0].IsDouble())
        return {Event::error, "value error"};

    if (document["tick"]["event"] == "snapshot")
      return {Event::snapshot, "success"};
    else
      return {Event::update, "success: "};
  }

  void set_snapshot(const rapidjson::Document& respond) {
    time = respond["ts"].GetUint64();
    chanel = respond["ch"].GetString();

    asks.clear();
    bids.clear();

    for (const auto& v : respond["tick"]["asks"].GetArray()) {
      if (v[1].GetInt() == 0) continue;

      asks[v[0].GetDouble()] = v[1].GetInt();
    }

    for (const auto& v : respond["tick"]["bids"].GetArray()) {
      if (v[1].GetInt() == 0) continue;

      bids[v[0].GetDouble()] = v[1].GetInt();
    }
  }

  void update_snapshot(const rapidjson::Document& respond) {
    time = respond["ts"].GetUint64();

    for (const auto& v : respond["tick"]["asks"].GetArray()) {
      if (v[1].GetInt() == 0) {
        asks.erase(v[0].GetDouble());
        continue;
      }
      asks[v[0].GetDouble()] = v[1].GetInt();
    }

    for (const auto& v : respond["tick"]["bids"].GetArray()) {
      if (v[1].GetInt() == 0) {
        bids.erase(v[0].GetDouble());
        continue;
      }
      bids[v[0].GetDouble()] = v[1].GetInt();
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
  std::string chanel = "";
  std::map<double, int, std::less<>> asks;
  std::map<double, int, std::greater<>> bids;
  const std::string members[3] = {"ch", "ts", "tick"};
  const std::string tick_members[3] = {"asks", "bids", "event"};
};

int main(int argc, char** argv) {
  std::ifstream input(argv[1]);
  std::ofstream output(argv[2]);

  if (!input.is_open() || !output.is_open()) return 1;

  Limit_order_book l;
  std::string s = "";

  std::chrono::duration<double, std::nano> summ_update_time;
  size_t update_counter = 0;
  std::chrono::_V2::steady_clock::time_point start;
  std::chrono::_V2::steady_clock::time_point end;
  std::chrono::duration<double, std::nano> diff;

  while (std::getline(input, s)) {
    auto [ev, doc] = l.process_data(s);

    if (ev == Event::snapshot) {
      l.set_snapshot(doc);

      auto [ask_price, ask_amount] = l.get_best_ask();
      auto [bid_price, bid_amount] = l.get_best_bid();

      output << std::fixed << std::setprecision(2) << "{" << l.get_time()
             << "}, {" << bid_price << "}, {" << bid_amount << "}, {"
             << ask_price << "}, {" << ask_amount << "}" << std::endl;

    } else if (ev == Event::update) {
      start = std::chrono::steady_clock::now();

      l.update_snapshot(doc);

      end = std::chrono::steady_clock::now();
      diff = end - start;
      std::cout << "update: " << diff.count() << " nanoseconds" << std::endl;

      auto [ask_price, ask_amount] = l.get_best_ask();
      auto [bid_price, bid_amount] = l.get_best_bid();

      output << std::fixed << std::setprecision(2) << "{" << l.get_time()
             << "}, {" << bid_price << "}, {" << bid_amount << "}, {"
             << ask_price << "}, {" << ask_amount << "}" << std::endl;

      ++update_counter;
      summ_update_time += diff;
    }
  }

  std::cout << "av update time: " << summ_update_time.count() / update_counter
            << " nanoseconds" << std::endl;

  return 0;
}