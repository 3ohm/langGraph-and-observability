#include "pick_planner.hpp"

#include <utility>

namespace warehouse {

PickPlanner::PickPlanner(std::vector<Aisle> aisles) : aisles_(std::move(aisles)) {}

int PickPlanner::route_seconds(const std::vector<PickTask>& tasks) const
{
    int total = 0;
    for (const PickTask& task : tasks) {
        const Aisle& aisle = aisles_.at(task.aisle);
        total += aisle.walk_seconds;
    }
    return total;
}

}  // namespace warehouse
