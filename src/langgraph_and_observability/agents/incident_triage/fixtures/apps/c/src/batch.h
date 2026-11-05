#ifndef ORDER_BATCHER_BATCH_H
#define ORDER_BATCHER_BATCH_H

#include <stddef.h>

struct order {
    int id;
    int warehouse_id;
    int quantity;
};

struct warehouse {
    int id;
    const char *code;
    int capacity;
};

void log_info(const char *fmt, ...);
const struct warehouse *find_warehouse(int id);
int process_batch(int warehouse_id, const struct order *orders, size_t count);

#endif
