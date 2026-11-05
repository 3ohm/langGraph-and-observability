#include "batch.h"

#include <stdarg.h>
#include <stdio.h>
#include <time.h>

static const struct warehouse WAREHOUSES[] = {
    {1, "LHR1", 1200},
    {2, "MAN2", 800},
    {3, "BHX1", 950},
};

void log_info(const char *fmt, ...)
{
    char stamp[32];
    time_t now = time(NULL);
    strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
    fprintf(stderr, "%s INFO ", stamp);
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fputc('\n', stderr);
}

const struct warehouse *find_warehouse(int id)
{
    for (size_t i = 0; i < sizeof WAREHOUSES / sizeof WAREHOUSES[0]; i++) {
        if (WAREHOUSES[i].id == id) {
            return &WAREHOUSES[i];
        }
    }
    return NULL;
}

int process_batch(int warehouse_id, const struct order *orders, size_t count)
{
    log_info("processing order batch for warehouse %d", warehouse_id);
    const struct warehouse *wh = find_warehouse(warehouse_id);
    int units = 0;
    for (size_t i = 0; i < count; i++) {
        units += orders[i].quantity;
    }
    if (units > wh->capacity) {
        log_info("batch for warehouse %s exceeds capacity: %d > %d units", wh->code, units, wh->capacity);
        return -1;
    }
    log_info("batch done for warehouse %s: %zu orders, %d units", wh->code, count, units);
    return units;
}
