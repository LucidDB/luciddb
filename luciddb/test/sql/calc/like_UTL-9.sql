-- The following sequence was inspired by bug 6633,
-- which I could not reproduce in 2.0.
set schema 's';

DROP TABLE french;
CREATE TABLE french (c VARCHAR(100));
INSERT INTO french (c) VALUES ('Société Titanité');
SELECT * FROM french;
SELECT * FROM french WHERE c LIKE '%Ti%';
