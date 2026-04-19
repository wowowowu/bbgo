-- +up
-- +begin
ALTER TABLE orders ADD COLUMN actual_order_id INTEGER NOT NULL DEFAULT 0;
-- +end

-- +down
-- +begin
ALTER TABLE orders DROP COLUMN actual_order_id;
-- +end
