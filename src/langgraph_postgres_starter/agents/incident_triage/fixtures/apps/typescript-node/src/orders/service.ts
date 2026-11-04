export interface Money {
  amount: number;
  currency: string;
}

export interface PaymentEvent {
  id: string;
  type: 'charge.succeeded' | 'charge.refunded';
  orderId: string;
  charge?: Money;
  refund?: Money;
}

export interface LedgerEntry {
  eventId: string;
  orderId: string;
  amountCents: number;
  currency: string;
}

export interface Ledger {
  write(entries: LedgerEntry[]): void;
}

export function toLedgerEntry(event: PaymentEvent): LedgerEntry {
  const sign = event.type === 'charge.refunded' ? -1 : 1;
  return {
    eventId: event.id,
    orderId: event.orderId,
    amountCents: sign * event.charge!.amount,
    currency: event.charge!.currency,
  };
}

export class SettlementService {
  private pending: PaymentEvent[] = [];

  constructor(private readonly ledger: Ledger) {}

  enqueue(event: PaymentEvent): void {
    this.pending.push(event);
  }

  flush(): void {
    if (this.pending.length === 0) {
      return;
    }
    const entries = this.pending.map(toLedgerEntry);
    this.pending = [];
    this.ledger.write(entries);
    console.log(`settled ${entries.length} payment events`);
  }
}
