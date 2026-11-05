#include "pick_planner.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

namespace fs = std::filesystem;

static std::vector<warehouse::PickTask> read_pick_list(const fs::path& path)
{
    std::vector<warehouse::PickTask> tasks;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream fields(line);
        warehouse::PickTask task;
        if (fields >> task.sku >> task.aisle >> task.quantity) {
            tasks.push_back(task);
        }
    }
    return tasks;
}

int main()
{
    const warehouse::PickPlanner planner({
        {"A01", 14}, {"A02", 18}, {"A03", 22}, {"A04", 26}, {"A05", 30}, {"A06", 34},
        {"A07", 38}, {"A08", 42}, {"A09", 46}, {"A10", 50}, {"A11", 54}, {"A12", 58},
    });
    const fs::path inbox = "/var/spool/picks";
    std::cout << "pick-planner watching " << inbox.string() << std::endl;
    for (;;) {
        for (const auto& entry : fs::directory_iterator(inbox)) {
            const auto tasks = read_pick_list(entry.path());
            const int seconds = planner.route_seconds(tasks);
            std::cout << "pick list " << entry.path().filename().string() << ": " << tasks.size()
                      << " tasks, route " << seconds << "s" << std::endl;
            fs::remove(entry.path());
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}
