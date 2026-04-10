-- Tradfy — Supabase SQL Schema
-- Run this in the Supabase SQL Editor

-- ─── Trades table ────────────────────────────────────────────────────────────
create table if not exists trades (
  id               uuid primary key default gen_random_uuid(),
  user_id          uuid not null references auth.users(id) on delete cascade,
  symbol           text,
  instrument_type  text check (instrument_type in ('equity','futures','options','currency','commodity')),
  action           text check (action in ('buy','sell')),
  quantity         integer,
  entry_price      numeric(12, 4),
  exit_price       numeric(12, 4),
  pnl              numeric(12, 2),
  pnl_percent      numeric(8, 4),
  trade_date       date,
  broker           text,
  ai_feedback      text,
  created_at       timestamptz not null default now()
);

-- ─── Indexes ─────────────────────────────────────────────────────────────────
create index if not exists trades_user_id_idx on trades(user_id);
create index if not exists trades_created_at_idx on trades(created_at desc);
create index if not exists trades_trade_date_idx on trades(trade_date desc);

-- ─── Row Level Security ───────────────────────────────────────────────────────
alter table trades enable row level security;

-- Users can only read their own trades
create policy "users_select_own_trades"
  on trades for select
  using (auth.uid() = user_id);

-- Users can only insert trades for themselves
create policy "users_insert_own_trades"
  on trades for insert
  with check (auth.uid() = user_id);

-- Users can only update their own trades
create policy "users_update_own_trades"
  on trades for update
  using (auth.uid() = user_id);

-- Users can only delete their own trades
create policy "users_delete_own_trades"
  on trades for delete
  using (auth.uid() = user_id);

-- ─── Swing trade columns (run these if trades table already exists) ───────────
-- Safe to run multiple times — adds columns only if they don't exist yet

alter table trades
  add column if not exists trade_type       text check (trade_type in ('options_intraday','equity_swing','futures_swing')),
  add column if not exists status           text check (status in ('open','closed')),
  add column if not exists sector           text check (sector in ('IT','Banking','Pharma','Auto','FMCG','Energy','Metals','Telecom','Realty')),
  add column if not exists overnight_charges numeric(10, 2);

-- Index for swing trade queries (filtering by trade_type + sector)
create index if not exists trades_trade_type_idx on trades(trade_type);
create index if not exists trades_sector_idx     on trades(sector);

-- ─── Open/close position tracking (run these if trades table already exists) ──

alter table trades
  add column if not exists linked_trade_id  uuid references trades(id) on delete set null,
  add column if not exists holding_days     integer,
  add column if not exists closed_at        timestamptz;

create index if not exists trades_status_idx          on trades(status);
create index if not exists trades_linked_trade_id_idx on trades(linked_trade_id);
