-- ============================================================
-- LegacyBridge AI — TARGET DATABASE (Modern PostgreSQL)
-- ============================================================
-- This represents the target system after ETL migration.
-- Drift is applied per the README mismatch matrix:
--   1. vendor: vendor_tier column MISSING, active_flag → BOOLEAN
--   2. inventory: unit_cost → FLOAT (rounding), timestamps +5hr TZ drift
--   3. purchase_order: CLEAN — identical to source
--   4. inventory_transaction: soft-deleted rows (status_code='DEL') OMITTED,
--      is_deleted BOOLEAN column added instead of status_code
--   5. supplier_contract: empty string → NULL, auto_renew → BOOLEAN
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- Table 1: vendor
-- ❌ MISSING: vendor_tier column dropped during migration
-- ❌ TYPE CHANGE: active_flag CHAR(1) → BOOLEAN
-- ❌ CDC GAP: BATCH_JOB rows have stale data (not captured)
-- ────────────────────────────────────────────────────────────
CREATE TABLE vendor (
    vendor_id        VARCHAR(10)   PRIMARY KEY,
    vendor_name      VARCHAR(100)  NOT NULL,
    -- vendor_tier MISSING ❌
    payment_terms    INT           NOT NULL,
    lead_time_days   INT           NOT NULL,
    active_flag      BOOLEAN       NOT NULL,           -- ❌ Was CHAR(1) in source
    country_code     CHAR(2)       NOT NULL,
    currency_code    CHAR(3)       NOT NULL,
    created_ts       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_ts       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by       VARCHAR(20)   NOT NULL DEFAULT 'SYSTEM'
);

-- BATCH_JOB rows have STALE updated_ts (CDC missed these updates)
INSERT INTO vendor (vendor_id, vendor_name, payment_terms, lead_time_days, active_flag, country_code, currency_code, created_ts, updated_ts, updated_by) VALUES
('V001', 'Acme Industrial Supply', 30, 14, true, 'US', 'USD', '2023-01-15 08:00:00', '2023-11-20 10:00:00', 'BATCH_JOB'),   -- ❌ stale ts (CDC gap)
('V002', 'GlobalTech Components', 45, 21, true, 'DE', 'EUR', '2023-02-01 09:30:00', '2024-01-12 09:15:00', 'USER_042'),
('V003', 'Pacific Rim Electronics', 60, 35, true, 'JP', 'JPY', '2023-02-15 10:00:00', '2024-01-08 16:45:00', 'USER_017'),
('V004', 'Nordic Precision Parts', 30, 18, false, 'SE', 'SEK', '2023-03-01 08:15:00', '2023-12-01 08:00:00', 'BATCH_JOB'),  -- ❌ stale ts (CDC gap)
('V005', 'Southern Cross Materials', 90, 42, true, 'AU', 'AUD', '2023-03-10 11:00:00', '2024-01-05 12:00:00', 'USER_008'),
('V006', 'Rhine Valley Chemicals', 30, 10, true, 'DE', 'EUR', '2023-04-01 08:00:00', '2023-10-15 14:00:00', 'BATCH_JOB'),   -- ❌ stale ts (CDC gap)
('V007', 'MidWest Steel Corp', 45, 28, true, 'US', 'USD', '2023-04-15 09:00:00', '2024-01-09 08:45:00', 'USER_023'),
('V008', 'Shanghai Fasteners Ltd', 60, 30, false, 'CN', 'CNY', '2023-05-01 10:30:00', '2023-11-05 09:30:00', 'BATCH_JOB'),  -- ❌ stale ts (CDC gap)
('V009', 'Atlas Bearings Group', 30, 12, true, 'UK', 'GBP', '2023-05-15 08:00:00', '2024-01-11 10:00:00', 'USER_055'),
('V010', 'Maple Leaf Timber Inc', 90, 45, true, 'CA', 'CAD', '2023-06-01 09:00:00', '2024-01-07 14:10:00', 'USER_012'),
('V011', 'Sahara Mining Corp', 60, 38, false, 'ZA', 'ZAR', '2023-06-15 10:00:00', '2023-12-20 11:00:00', 'BATCH_JOB'),     -- ❌ stale ts (CDC gap)
('V012', 'Baltic Shipping Supplies', 45, 22, true, 'PL', 'PLN', '2023-07-01 08:30:00', '2024-01-06 09:00:00', 'USER_031'),
('V013', 'Andean Copper Works', 30, 15, true, 'CL', 'CLP', '2023-07-15 11:00:00', '2023-11-10 12:00:00', 'BATCH_JOB'),      -- ❌ stale ts (CDC gap)
('V014', 'Great Plains Polymers', 45, 25, true, 'US', 'USD', '2023-08-01 08:00:00', '2024-01-10 13:30:00', 'USER_044'),
('V015', 'Fjord Hydraulics AS', 30, 16, false, 'NO', 'NOK', '2023-08-15 09:30:00', '2023-10-28 07:00:00', 'BATCH_JOB'),     -- ❌ stale ts (CDC gap)
('V016', 'Bengal Textiles Ltd', 90, 50, true, 'IN', 'INR', '2023-09-01 10:00:00', '2024-01-08 08:00:00', 'USER_019'),
('V017', 'Danube Precision GmbH', 30, 11, true, 'AT', 'EUR', '2023-09-15 08:00:00', '2023-12-05 13:00:00', 'BATCH_JOB'),    -- ❌ stale ts (CDC gap)
('V018', 'Cascadia Electronics', 60, 32, true, 'US', 'USD', '2023-10-01 09:00:00', '2024-01-09 10:30:00', 'USER_027'),
('V019', 'Iberian Steel SA', 45, 20, false, 'ES', 'EUR', '2023-10-15 10:30:00', '2023-11-18 15:00:00', 'BATCH_JOB'),        -- ❌ stale ts (CDC gap)
('V020', 'Outback Industrial Pty', 90, 40, true, 'AU', 'AUD', '2023-11-01 08:00:00', '2024-01-07 15:20:00', 'USER_006');

-- ────────────────────────────────────────────────────────────
-- Table 2: inventory
-- ⚠️ unit_cost: FLOAT instead of DECIMAL(10,4) → rounding loss
-- ⚠️ last_counted_ts: +5 hour TZ drift applied
-- ────────────────────────────────────────────────────────────
CREATE TABLE inventory (
    item_id          VARCHAR(15)    PRIMARY KEY,
    location_id      VARCHAR(10)    NOT NULL,
    on_hand_qty      DECIMAL(10,2)  NOT NULL,
    reorder_point    INT            NOT NULL,
    reorder_qty      INT            NOT NULL,
    unit_cost        FLOAT          NOT NULL,          -- ⚠️ Was DECIMAL(10,4) → rounding loss
    status_code      CHAR(2)        NOT NULL,
    last_counted_ts  TIMESTAMP      NOT NULL,           -- ⚠️ +5hr TZ drift applied below
    updated_ts       TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by       VARCHAR(20)    NOT NULL DEFAULT 'SYSTEM'
);

-- Note: unit_cost values show FLOAT rounding artifacts
-- Note: last_counted_ts shifted +5 hours from source (TZ drift)
INSERT INTO inventory (item_id, location_id, on_hand_qty, reorder_point, reorder_qty, unit_cost, status_code, last_counted_ts, updated_ts, updated_by) VALUES
('ITM-000001', 'WH-EAST', 1500.00, 200, 500, 12.345600128173828, 'AC', '2024-01-10 13:00:00', '2024-01-10 08:00:00', 'COUNTER_01'),
('ITM-000002', 'WH-EAST', 340.50, 100, 250, 45.678901672363281, 'AC', '2024-01-10 13:30:00', '2024-01-10 08:30:00', 'COUNTER_01'),
('ITM-000003', 'WH-WEST', 0.00, 50, 100, 8.123399734497070, 'OO', '2024-01-09 19:00:00', '2024-01-09 14:00:00', 'COUNTER_02'),
('ITM-000004', 'WH-WEST', 2200.75, 300, 600, 3.456700086593628, 'AC', '2024-01-10 14:00:00', '2024-01-10 09:00:00', 'COUNTER_02'),
('ITM-000005', 'WH-NORTH', 125.00, 50, 200, 99.999900817871094, 'AC', '2024-01-08 15:00:00', '2024-01-08 10:00:00', 'COUNTER_03'),
('ITM-000006', 'WH-NORTH', 890.25, 150, 400, 22.334400177001953, 'AC', '2024-01-10 15:30:00', '2024-01-10 10:30:00', 'COUNTER_03'),
('ITM-000007', 'WH-SOUTH', 45.00, 20, 50, 156.789093017578125, 'LW', '2024-01-07 16:00:00', '2024-01-07 11:00:00', 'COUNTER_04'),
('ITM-000008', 'WH-SOUTH', 3100.00, 500, 1000, 1.234499931335449, 'AC', '2024-01-10 16:30:00', '2024-01-10 11:30:00', 'COUNTER_04'),
('ITM-000009', 'WH-EAST', 670.50, 100, 300, 67.891197204589844, 'AC', '2024-01-10 17:00:00', '2024-01-10 12:00:00', 'COUNTER_01'),
('ITM-000010', 'WH-EAST', 15.00, 10, 25, 234.567794799804688, 'LW', '2024-01-06 18:00:00', '2024-01-06 13:00:00', 'COUNTER_01'),
('ITM-000011', 'WH-WEST', 4500.00, 600, 1200, 0.567800045013428, 'AC', '2024-01-10 18:30:00', '2024-01-10 13:30:00', 'COUNTER_02'),
('ITM-000012', 'WH-WEST', 280.00, 80, 160, 33.333301544189453, 'AC', '2024-01-10 19:00:00', '2024-01-10 14:00:00', 'COUNTER_02'),
('ITM-000013', 'WH-NORTH', 0.00, 25, 75, 78.901199340820313, 'OO', '2024-01-05 20:00:00', '2024-01-05 15:00:00', 'COUNTER_03'),
('ITM-000014', 'WH-NORTH', 1750.25, 250, 500, 5.678900241851807, 'AC', '2024-01-10 20:30:00', '2024-01-10 15:30:00', 'COUNTER_03'),
('ITM-000015', 'WH-SOUTH', 92.00, 30, 60, 445.671203613281250, 'AC', '2024-01-10 21:00:00', '2024-01-10 16:00:00', 'COUNTER_04'),
('ITM-000016', 'WH-EAST', 1100.00, 200, 400, 18.901199340820313, 'AC', '2024-01-10 13:15:00', '2024-01-10 08:15:00', 'COUNTER_01'),
('ITM-000017', 'WH-WEST', 560.75, 100, 250, 55.555500030517578, 'AC', '2024-01-10 14:15:00', '2024-01-10 09:15:00', 'COUNTER_02'),
('ITM-000018', 'WH-NORTH', 33.00, 15, 30, 189.012298583984375, 'LW', '2024-01-04 15:00:00', '2024-01-04 10:00:00', 'COUNTER_03'),
('ITM-000019', 'WH-SOUTH', 2800.50, 400, 800, 2.345600128173828, 'AC', '2024-01-10 16:45:00', '2024-01-10 11:45:00', 'COUNTER_04'),
('ITM-000020', 'WH-EAST', 410.00, 75, 150, 71.234497070312500, 'AC', '2024-01-10 17:30:00', '2024-01-10 12:30:00', 'COUNTER_01');

-- ────────────────────────────────────────────────────────────
-- Table 3: purchase_order
-- ✅ CLEAN TABLE — identical to source. Healthy baseline.
-- ────────────────────────────────────────────────────────────
CREATE TABLE purchase_order (
    po_number        VARCHAR(20)    PRIMARY KEY,
    vendor_id        VARCHAR(10)    NOT NULL REFERENCES vendor(vendor_id),
    order_date       DATE           NOT NULL,
    expected_date    DATE           NOT NULL,
    total_amount     DECIMAL(15,2)  NOT NULL,
    po_status        VARCHAR(20)    NOT NULL,
    line_count       INT            NOT NULL,
    approved_by      VARCHAR(30)    NOT NULL,
    created_ts       TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_ts       TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO purchase_order (po_number, vendor_id, order_date, expected_date, total_amount, po_status, line_count, approved_by, created_ts, updated_ts) VALUES
('PO-2024-00001', 'V001', '2024-01-02', '2024-01-16', 45230.50, 'DELIVERED', 5, 'MGR_JOHNSON', '2024-01-02 08:00:00', '2024-01-16 14:00:00'),
('PO-2024-00002', 'V002', '2024-01-03', '2024-01-24', 12890.00, 'DELIVERED', 3, 'MGR_SCHMIDT', '2024-01-03 09:15:00', '2024-01-24 10:30:00'),
('PO-2024-00003', 'V003', '2024-01-05', '2024-02-09', 78340.25, 'IN_TRANSIT', 8, 'DIR_TANAKA', '2024-01-05 10:00:00', '2024-01-20 16:00:00'),
('PO-2024-00004', 'V005', '2024-01-06', '2024-02-17', 5670.00, 'APPROVED', 2, 'MGR_WILSON', '2024-01-06 08:30:00', '2024-01-06 14:00:00'),
('PO-2024-00005', 'V001', '2024-01-08', '2024-01-22', 23100.75, 'DELIVERED', 4, 'MGR_JOHNSON', '2024-01-08 09:00:00', '2024-01-22 11:00:00'),
('PO-2024-00006', 'V009', '2024-01-09', '2024-01-21', 91200.00, 'DELIVERED', 12, 'VP_OPERATIONS', '2024-01-09 08:00:00', '2024-01-21 15:30:00'),
('PO-2024-00007', 'V006', '2024-01-10', '2024-01-20', 34560.80, 'DELIVERED', 6, 'MGR_MULLER', '2024-01-10 10:30:00', '2024-01-20 09:00:00'),
('PO-2024-00008', 'V012', '2024-01-11', '2024-02-01', 18900.50, 'IN_TRANSIT', 3, 'MGR_KOWALSKI', '2024-01-11 08:45:00', '2024-01-18 12:00:00'),
('PO-2024-00009', 'V007', '2024-01-12', '2024-02-09', 67800.00, 'APPROVED', 7, 'DIR_SUPPLY', '2024-01-12 09:30:00', '2024-01-12 16:00:00'),
('PO-2024-00010', 'V014', '2024-01-13', '2024-02-07', 8450.25, 'PENDING', 2, 'MGR_JOHNSON', '2024-01-13 11:00:00', '2024-01-13 11:00:00'),
('PO-2024-00011', 'V017', '2024-01-14', '2024-01-25', 112500.00, 'DELIVERED', 15, 'VP_OPERATIONS', '2024-01-14 08:00:00', '2024-01-25 14:00:00'),
('PO-2024-00012', 'V004', '2024-01-15', '2024-02-02', 29300.60, 'IN_TRANSIT', 4, 'MGR_LINDBERG', '2024-01-15 09:00:00', '2024-01-20 10:00:00'),
('PO-2024-00013', 'V010', '2024-01-15', '2024-02-28', 4200.00, 'APPROVED', 1, 'MGR_WILSON', '2024-01-15 10:00:00', '2024-01-15 15:00:00'),
('PO-2024-00014', 'V013', '2024-01-15', '2024-01-30', 56700.90, 'DELIVERED', 9, 'DIR_SUPPLY', '2024-01-15 08:30:00', '2024-01-30 13:00:00'),
('PO-2024-00015', 'V018', '2024-01-15', '2024-02-14', 15800.00, 'PENDING', 3, 'MGR_JOHNSON', '2024-01-15 14:00:00', '2024-01-15 14:00:00');

-- ────────────────────────────────────────────────────────────
-- Table 4: inventory_transaction
-- ❌ SOFT DELETE MISMATCH: status_code replaced with is_deleted BOOLEAN
-- ❌ Rows with status_code='DEL' in source are OMITTED here
-- ⚠️ txn_ts has +5hr TZ drift
-- ────────────────────────────────────────────────────────────
CREATE TABLE inventory_transaction (
    txn_id           BIGINT         PRIMARY KEY,
    item_id          VARCHAR(15)    NOT NULL REFERENCES inventory(item_id),
    txn_type         CHAR(3)        NOT NULL,
    txn_qty          DECIMAL(10,2)  NOT NULL,
    txn_ts           TIMESTAMP      NOT NULL,           -- ⚠️ +5hr TZ drift
    location_id      VARCHAR(10)    NOT NULL,
    reference_id     VARCHAR(30),
    created_by       VARCHAR(20)    NOT NULL,
    is_deleted       BOOLEAN        NOT NULL DEFAULT false  -- ❌ Replaced status_code CHAR(3)
);

-- Only ACT rows from source are present; DEL rows (1000006, 1000009, 1000012, 1000015, 1000020, 1000025) are MISSING
-- txn_ts shifted +5 hours from source
INSERT INTO inventory_transaction (txn_id, item_id, txn_type, txn_qty, txn_ts, location_id, reference_id, created_by, is_deleted) VALUES
(1000001, 'ITM-000001', 'RCV', 500.00, '2024-01-10 13:30:00', 'WH-EAST', 'PO-2024-00001', 'DOCK_WORKER_01', false),
(1000002, 'ITM-000001', 'SAL', -50.00, '2024-01-10 14:15:00', 'WH-EAST', 'SO-88001', 'PICK_WORKER_03', false),
(1000003, 'ITM-000002', 'RCV', 200.00, '2024-01-10 15:00:00', 'WH-EAST', 'PO-2024-00002', 'DOCK_WORKER_01', false),
(1000004, 'ITM-000003', 'ADJ', -25.00, '2024-01-09 19:30:00', 'WH-WEST', 'CYCLE-COUNT-009', 'COUNTER_02', false),
(1000005, 'ITM-000004', 'TRF', 100.00, '2024-01-10 16:00:00', 'WH-WEST', 'TRF-EAST-WEST-01', 'LOGISTICS_05', false),
-- 1000006 MISSING (was DEL in source) ❌
(1000007, 'ITM-000005', 'RCV', 75.00, '2024-01-08 16:00:00', 'WH-NORTH', 'PO-2024-00005', 'DOCK_WORKER_03', false),
(1000008, 'ITM-000006', 'SAL', -200.00, '2024-01-10 17:00:00', 'WH-NORTH', 'SO-88003', 'PICK_WORKER_07', false),
-- 1000009 MISSING (was DEL in source) ❌
(1000010, 'ITM-000008', 'RCV', 1000.00, '2024-01-10 18:00:00', 'WH-SOUTH', 'PO-2024-00006', 'DOCK_WORKER_04', false),
(1000011, 'ITM-000009', 'SAL', -30.00, '2024-01-10 19:00:00', 'WH-EAST', 'SO-88004', 'PICK_WORKER_03', false),
-- 1000012 MISSING (was DEL in source) ❌
(1000013, 'ITM-000011', 'RCV', 2000.00, '2024-01-10 19:30:00', 'WH-WEST', 'PO-2024-00007', 'DOCK_WORKER_02', false),
(1000014, 'ITM-000012', 'SAL', -40.00, '2024-01-10 20:00:00', 'WH-WEST', 'SO-88005', 'PICK_WORKER_06', false),
-- 1000015 MISSING (was DEL in source) ❌
(1000016, 'ITM-000014', 'RCV', 750.00, '2024-01-10 21:30:00', 'WH-NORTH', 'PO-2024-00011', 'DOCK_WORKER_03', false),
(1000017, 'ITM-000015', 'SAL', -8.00, '2024-01-10 22:00:00', 'WH-SOUTH', 'SO-88006', 'PICK_WORKER_09', false),
(1000018, 'ITM-000016', 'TRF', 300.00, '2024-01-10 13:45:00', 'WH-EAST', 'TRF-SOUTH-EAST-03', 'LOGISTICS_05', false),
(1000019, 'ITM-000017', 'RCV', 400.00, '2024-01-10 14:45:00', 'WH-WEST', 'PO-2024-00008', 'DOCK_WORKER_02', false),
-- 1000020 MISSING (was DEL in source) ❌
(1000021, 'ITM-000019', 'RCV', 800.00, '2024-01-10 17:15:00', 'WH-SOUTH', 'PO-2024-00012', 'DOCK_WORKER_04', false),
(1000022, 'ITM-000020', 'SAL', -60.00, '2024-01-10 18:30:00', 'WH-EAST', 'SO-88007', 'PICK_WORKER_03', false),
(1000023, 'ITM-000001', 'ADJ', -12.00, '2024-01-11 14:00:00', 'WH-EAST', 'CYCLE-COUNT-013', 'COUNTER_01', false),
(1000024, 'ITM-000004', 'SAL', -350.00, '2024-01-11 15:00:00', 'WH-WEST', 'SO-88008', 'PICK_WORKER_06', false);
-- 1000025 MISSING (was DEL in source) ❌

-- ────────────────────────────────────────────────────────────
-- Table 5: supplier_contract
-- ⚠️ terms_notes: empty strings from source become NULL
-- ⚠️ auto_renew: CHAR(1) Y/N → BOOLEAN
-- ────────────────────────────────────────────────────────────
CREATE TABLE supplier_contract (
    contract_id      VARCHAR(20)    PRIMARY KEY,
    vendor_id        VARCHAR(10)    NOT NULL REFERENCES vendor(vendor_id),
    start_date       DATE           NOT NULL,
    end_date         DATE           NOT NULL,
    contract_value   DECIMAL(15,2)  NOT NULL,
    terms_notes      VARCHAR(500),                     -- ⚠️ Empty strings became NULL
    payment_freq     VARCHAR(20)    NOT NULL,
    auto_renew       BOOLEAN        NOT NULL,           -- ⚠️ Was CHAR(1) Y/N in source
    created_ts       TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO supplier_contract (contract_id, vendor_id, start_date, end_date, contract_value, terms_notes, payment_freq, auto_renew, created_ts) VALUES
('CTR-2023-001', 'V001', '2023-01-01', '2025-12-31', 500000.00, 'Net 30, 2% early payment discount. FOB destination.', 'MONTHLY', true, '2022-12-15 08:00:00'),
('CTR-2023-002', 'V002', '2023-02-01', '2024-12-31', 250000.00, NULL, 'QUARTERLY', true, '2023-01-20 09:00:00'),           -- ⚠️ was empty string
('CTR-2023-003', 'V003', '2023-03-01', '2025-02-28', 780000.00, 'Includes warranty coverage. Penalties for late delivery > 5 days.', 'MONTHLY', false, '2023-02-15 10:00:00'),
('CTR-2023-004', 'V006', '2023-04-01', '2025-03-31', 320000.00, NULL, 'MONTHLY', true, '2023-03-18 08:30:00'),              -- ⚠️ was empty string
('CTR-2023-005', 'V009', '2023-05-01', '2026-04-30', 1200000.00, 'Strategic partnership. Volume rebates at 10K+ units. Annual review clause.', 'MONTHLY', true, '2023-04-10 09:00:00'),
('CTR-2023-006', 'V004', '2023-06-01', '2024-05-31', 150000.00, NULL, 'QUARTERLY', false, '2023-05-20 10:30:00'),           -- ⚠️ was empty string
('CTR-2023-007', 'V012', '2023-07-01', '2025-06-30', 410000.00, 'Includes logistics and customs handling. EUR denominated.', 'MONTHLY', true, '2023-06-15 08:00:00'),
('CTR-2023-008', 'V005', '2023-08-01', '2024-07-31', 95000.00, NULL, 'SEMI_ANNUAL', false, '2023-07-20 09:15:00'),          -- ⚠️ was empty string
('CTR-2023-009', 'V013', '2023-09-01', '2025-08-31', 670000.00, 'Copper price indexed. Quarterly price adjustments per LME.', 'MONTHLY', true, '2023-08-12 10:00:00'),
('CTR-2023-010', 'V017', '2023-10-01', '2026-09-30', 890000.00, 'Precision parts. Zero-defect clause with financial penalties.', 'MONTHLY', true, '2023-09-18 08:00:00'),
('CTR-2023-011', 'V007', '2023-10-15', '2025-10-14', 230000.00, NULL, 'QUARTERLY', false, '2023-09-30 09:00:00'),           -- ⚠️ was empty string
('CTR-2023-012', 'V014', '2023-11-01', '2025-10-31', 180000.00, 'Standard terms. No volume commitments.', 'MONTHLY', false, '2023-10-15 10:00:00');
