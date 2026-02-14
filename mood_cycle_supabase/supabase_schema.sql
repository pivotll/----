-- 创建 emotion_cycle 表
create table if not exists emotion_cycle (
  trade_date date primary key,
  up_count int,
  down_count int,
  up5_count int,
  down5_count int,
  limit_up_count int,
  limit_down_count int,
  break_count int,
  break_rate float,
  first_board int,
  second_board int,
  third_board int,
  above_third int,
  max_board int,
  fanpao_count int,
  limit_amount float,
  seal_amount float,
  first_red_rate float,
  first_premium float,
  second_red_rate float,
  second_premium float,
  third_red_rate float,
  third_premium float,
  third_plus_red_rate float,
  third_plus_premium float,
  advance_1to2 float,
  advance_2to3 float,
  advance_3to4 float,
  advance_3plus float,
  created_at timestamp with time zone default timezone('utc'::text, now()) not null
);

-- 开启 Row Level Security (RLS)
alter table emotion_cycle enable row level security;

-- 创建策略：允许所有用户读取（如果是公开网站）
create policy "Allow public read access"
  on emotion_cycle for select
  using (true);

-- 创建策略：仅允许特定用户或服务角色写入（这里为了简单，暂时允许匿名写入，生产环境建议限制）
-- 或者如果你在后端使用 service_role key，则不需要 RLS 策略也能写入
-- create policy "Allow anon insert" on emotion_cycle for insert with check (true);
-- create policy "Allow anon update" on emotion_cycle for update using (true);
