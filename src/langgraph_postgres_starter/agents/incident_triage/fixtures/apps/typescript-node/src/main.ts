import { createServer } from 'node:http';

import { type LedgerEntry, type PaymentEvent, SettlementService } from './orders/service';

const FLUSH_INTERVAL_MS = 5_000;
const port = Number(process.env.PORT ?? 8080);

const service = new SettlementService({
  write(entries: LedgerEntry[]): void {
    for (const entry of entries) {
      process.stdout.write(`${JSON.stringify(entry)}\n`);
    }
  },
});

const server = createServer((req, res) => {
  if (req.method !== 'POST' || req.url !== '/webhooks/payments') {
    res.writeHead(404).end();
    return;
  }
  let body = '';
  req.on('data', (chunk: Buffer) => {
    body += chunk.toString('utf8');
  });
  req.on('end', () => {
    service.enqueue(JSON.parse(body) as PaymentEvent);
    res.writeHead(202).end();
  });
});

setInterval(() => service.flush(), FLUSH_INTERVAL_MS);
server.listen(port, () => console.log(`payment-settlement listening on :${port}`));
