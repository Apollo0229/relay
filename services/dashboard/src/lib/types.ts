/** The snapshot GET /v1/dashboard returns, and the live stream repeats every second. */
export interface Dashboard {
  events: number;
  pending: number;
  published: number;
  processed: number;
  retried: number;
  dead: number;
  sources: { source: string; count: number }[];
  dead_letters: DeadLetter[];
}

export interface DeadLetter {
  id: string;
  source: string;
  type: string;
  attempts: number;
  error: string | null;
  at: string;
}
