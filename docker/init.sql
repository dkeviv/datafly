-- Datafly Demo Database
-- Realistic SaaS company data for demonstrating context building

-- Customers
CREATE TABLE dim_customer (
    customer_id     SERIAL PRIMARY KEY,
    company_name    TEXT NOT NULL,
    plan            TEXT NOT NULL CHECK (plan IN ('starter', 'growth', 'enterprise')),
    status          TEXT NOT NULL CHECK (status IN ('active', 'churned', 'trial', 'suspended')),
    industry        TEXT,
    country         TEXT DEFAULT 'US',
    created_at      TIMESTAMP DEFAULT NOW(),
    trial_start     TIMESTAMP,
    converted_at    TIMESTAMP,
    churned_at      TIMESTAMP,
    salesforce_id   TEXT
);

-- Revenue fact table
CREATE TABLE fct_revenue (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES dim_customer(customer_id),
    fiscal_quarter  TEXT NOT NULL,  -- e.g. 'FY2025-Q1'
    fiscal_year     INTEGER NOT NULL,
    arr             NUMERIC(12,2) NOT NULL,
    mrr             NUMERIC(12,2) GENERATED ALWAYS AS (arr / 12) STORED,
    plan            TEXT,
    is_expansion    BOOLEAN DEFAULT FALSE,
    is_contraction  BOOLEAN DEFAULT FALSE,
    recorded_at     TIMESTAMP DEFAULT NOW()
);

-- Monthly active users
CREATE TABLE fct_user_activity (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES dim_customer(customer_id),
    user_email      TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    event_date      DATE NOT NULL,
    session_count   INTEGER DEFAULT 1
);

-- Churn events
CREATE TABLE fct_churn (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER REFERENCES dim_customer(customer_id),
    churned_at      DATE NOT NULL,
    reason          TEXT,
    arr_lost        NUMERIC(12,2),
    plan            TEXT
);

-- Seed customers
INSERT INTO dim_customer (company_name, plan, status, industry, country, created_at, converted_at)
VALUES
    ('Acme Corp',        'enterprise', 'active',  'Manufacturing',  'US', NOW() - INTERVAL '2 years', NOW() - INTERVAL '2 years'),
    ('Globex Inc',       'growth',     'active',  'Technology',     'US', NOW() - INTERVAL '18 months', NOW() - INTERVAL '17 months'),
    ('Initech',          'starter',    'active',  'Finance',        'US', NOW() - INTERVAL '6 months',  NOW() - INTERVAL '5 months'),
    ('Umbrella Ltd',     'enterprise', 'active',  'Healthcare',     'UK', NOW() - INTERVAL '3 years',   NOW() - INTERVAL '3 years'),
    ('Stark Industries', 'growth',     'churned', 'Defense',        'US', NOW() - INTERVAL '1 year',    NOW() - INTERVAL '11 months'),
    ('Wayne Enterprises','enterprise', 'active',  'Conglomerate',   'US', NOW() - INTERVAL '2 years',   NOW() - INTERVAL '2 years'),
    ('Pied Piper',       'starter',    'trial',   'Technology',     'US', NOW() - INTERVAL '14 days',   NULL),
    ('Hooli',            'growth',     'active',  'Technology',     'US', NOW() - INTERVAL '8 months',  NOW() - INTERVAL '7 months');

-- Seed revenue (FY ends Nov 30 — intentional quirk for demo)
INSERT INTO fct_revenue (customer_id, fiscal_quarter, fiscal_year, arr, plan)
VALUES
    (1, 'FY2025-Q1', 2025, 120000, 'enterprise'),
    (1, 'FY2025-Q2', 2025, 120000, 'enterprise'),
    (1, 'FY2025-Q3', 2025, 132000, 'enterprise'),  -- expansion
    (2, 'FY2025-Q1', 2025,  24000, 'growth'),
    (2, 'FY2025-Q2', 2025,  24000, 'growth'),
    (2, 'FY2025-Q3', 2025,  24000, 'growth'),
    (3, 'FY2025-Q2', 2025,   6000, 'starter'),
    (3, 'FY2025-Q3', 2025,   6000, 'starter'),
    (4, 'FY2025-Q1', 2025, 240000, 'enterprise'),
    (4, 'FY2025-Q2', 2025, 240000, 'enterprise'),
    (4, 'FY2025-Q3', 2025, 264000, 'enterprise'),
    (6, 'FY2025-Q1', 2025, 180000, 'enterprise'),
    (6, 'FY2025-Q2', 2025, 180000, 'enterprise'),
    (6, 'FY2025-Q3', 2025, 198000, 'enterprise'),
    (8, 'FY2025-Q2', 2025,  24000, 'growth'),
    (8, 'FY2025-Q3', 2025,  24000, 'growth');

-- Seed churn
INSERT INTO fct_churn (customer_id, churned_at, reason, arr_lost, plan)
VALUES
    (5, CURRENT_DATE - INTERVAL '2 months', 'Price sensitivity', 24000, 'growth');

-- Seed some user activity
INSERT INTO fct_user_activity (customer_id, user_email, event_type, event_date)
SELECT
    c.customer_id,
    LOWER(REPLACE(c.company_name, ' ', '.')) || '.user@example.com',
    'login',
    CURRENT_DATE - (random() * 30)::INTEGER
FROM dim_customer c, generate_series(1, 5) s
WHERE c.status = 'active';

-- Views (intentional — context agent should flag these as non-source-of-truth)
CREATE VIEW mv_revenue_monthly AS
SELECT
    customer_id,
    fiscal_year,
    fiscal_quarter,
    SUM(mrr) AS total_mrr
FROM fct_revenue
GROUP BY customer_id, fiscal_year, fiscal_quarter;

-- Simulate query history in pg_stat_statements style (comment for demo)
-- In real setup, pg_stat_statements is enabled and auto-populated

COMMENT ON TABLE fct_revenue IS 'Source of truth for all revenue metrics. Fiscal year ends November 30.';
COMMENT ON TABLE dim_customer IS 'Master customer table. Trials excluded from revenue calculations.';
COMMENT ON COLUMN fct_revenue.arr IS 'Annual Recurring Revenue in USD, excluding trials and refunds';
COMMENT ON COLUMN fct_revenue.fiscal_quarter IS 'Format: FY{year}-Q{1-4}. Q4 ends November 30.';
