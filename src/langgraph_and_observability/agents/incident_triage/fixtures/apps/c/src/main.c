#include "batch.h"

#include <stdio.h>
#include <unistd.h>

#define MAX_ORDERS 512

/* A batch file holds the warehouse ID on its first line, then one "order_id quantity" per line. */
static size_t read_batch(FILE *in, int *warehouse_id, struct order *orders, size_t max)
{
    size_t count = 0;
    if (fscanf(in, "%d", warehouse_id) != 1) {
        return 0;
    }
    while (count < max && fscanf(in, "%d %d", &orders[count].id, &orders[count].quantity) == 2) {
        orders[count].warehouse_id = *warehouse_id;
        count++;
    }
    return count;
}

int main(int argc, char **argv)
{
    const char *spool = argc > 1 ? argv[1] : "/var/spool/orders/next.batch";
    static struct order orders[MAX_ORDERS];
    log_info("order-batcher started, spool %s", spool);
    for (;;) {
        FILE *in = fopen(spool, "r");
        if (in == NULL) {
            sleep(5);
            continue;
        }
        int warehouse_id = 0;
        size_t count = read_batch(in, &warehouse_id, orders, MAX_ORDERS);
        fclose(in);
        unlink(spool);
        if (count > 0) {
            process_batch(warehouse_id, orders, count);
        }
    }
}
