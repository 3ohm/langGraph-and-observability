#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace warehouse {

struct PickTask {
    std::string sku;
    std::size_t aisle;  // As signed on the warehouse floor: 1, 2, 3...
    int quantity;
};

struct Aisle {
    std::string label;
    int walk_seconds;
};

// Plans pick routes through the aisles, which are listed nearest to the packing stations first.
class PickPlanner {
public:
    explicit PickPlanner(std::vector<Aisle> aisles);
    int route_seconds(const std::vector<PickTask>& tasks) const;

private:
    std::vector<Aisle> aisles_;
};

}  // namespace warehouse
